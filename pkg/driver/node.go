/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/mount-utils"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/cloud"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/driver/internal"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/util"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

var (
	nodeCaps = []csi.NodeServiceCapability_RPC_Type{}

	// taintRemovalBackoff is the exponential backoff configuration for node taint removal
	taintRemovalBackoff = wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   2,
		Steps:    10, // Max delay = 0.5 * 2^9 = ~4 minutes
	}
)

// VolumeOperationAlreadyExists is message fmt returned to CO when there is another in-flight call on the given rpcKey
const VolumeOperationAlreadyExists = "An operation with the given volume=%q and target=%q is already in progress"

type nodeService struct {
	mounter       Mounter
	inFlight      *internal.InFlight
	driverOptions *DriverOptions
	csi.UnimplementedNodeServer
}

func newNodeService(driverOptions *DriverOptions) nodeService {
	klog.V(5).InfoS("[Debug] Retrieving node info from metadata service")
	region := os.Getenv("AWS_REGION")
	if region == "" {
		klog.InfoS("newNodeService: getting region from metadata")
		metadata, err := cloud.NewMetadataService(cloud.DefaultEC2MetadataClient, cloud.DefaultKubernetesAPIClient, region)
		if err != nil {
			panic(err)
		}
		region = metadata.GetRegion()
	}

	klog.InfoS("regionFromSession Node service", "region", region)

	nodeMounter, err := newNodeMounter()
	if err != nil {
		panic(err)
	}

	// Remove taint from node to indicate driver startup success
	// This is done in the background as a goroutine to allow for driver startup
	go removeTaintInBackground(cloud.DefaultKubernetesAPIClient, removeNotReadyTaint)

	return nodeService{
		mounter:       nodeMounter,
		inFlight:      internal.NewInFlight(),
		driverOptions: driverOptions,
	}
}

func (d *nodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *nodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *nodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.V(4).InfoS("NodePublishVolume: called with", "args", util.SanitizeRequest(req))

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	context := req.GetVolumeContext()
	dnsname := context[volumeContextDnsName]
	mountname := context[volumeContextMountName]

	if len(dnsname) == 0 {
		return nil, status.Error(codes.InvalidArgument, "dnsname is not provided")
	}

	if len(mountname) == 0 {
		mountname = "fsx"
	}

	source := fmt.Sprintf("%s@tcp:/%s", dnsname, mountname)

	target := req.GetTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volCap}) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	rpcKey := fmt.Sprintf("%s-%s", volumeID, target)

	if ok := d.inFlight.Insert(rpcKey); !ok {
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExists, volumeID, target)
	}
	defer func() {
		klog.V(4).InfoS("NodePublishVolume: volume operation finished", "rpcKey", rpcKey)
		d.inFlight.Delete(rpcKey)
	}()

	mountOptions := []string{}
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	if m := volCap.GetMount(); m != nil {
		hasOption := func(options []string, opt string) bool {
			for _, o := range options {
				if o == opt {
					return true
				}
			}
			return false
		}
		for _, f := range m.MountFlags {
			if !hasOption(mountOptions, f) {
				mountOptions = append(mountOptions, f)
			}
		}
	}
	klog.V(5).InfoS("NodePublishVolume: creating", "dir", target)
	if err := d.mounter.MakeDir(target); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not create dir %q: %v", target, err)
	}

	//Checking if the target directory is already mounted with a volume.
	mounted, err := d.isMounted(source, target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not check if %q is mounted: %v", target, err)
	}
	if !mounted {
		klog.V(4).InfoS("NodePublishVolume: mounting", "source", source, "target", target, "mountOptions", mountOptions)
		if err := d.mounter.Mount(source, target, "lustre", mountOptions); err != nil {
			os.Remove(target)
			return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", source, target, err)
		}
		klog.V(5).InfoS("NodePublishVolume: was mounted", "target", target)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *nodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(4).InfoS("NodeUnpublishVolume: called", "args", util.SanitizeRequest(req))

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}
	target := req.GetTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	rpcKey := fmt.Sprintf("%s-%s", volumeID, target)
	if ok := d.inFlight.Insert(rpcKey); !ok {
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExists, volumeID, target)
	}
	defer func() {
		klog.V(4).InfoS("NodeUnpublishVolume: volume operation finished", "rpcKey", rpcKey)
		d.inFlight.Delete(rpcKey)
	}()

	// Check if the target is mounted before unmounting
	notMnt, _ := d.mounter.IsLikelyNotMountPoint(target)
	if notMnt {
		klog.V(5).InfoS("NodeUnpublishVolume: target path not mounted, skipping unmount", "target", target)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	klog.V(5).InfoS("NodeUnpublishVolume: unmounting", "target", target)
	if err := d.unmount(target); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not unmount %q: %v", target, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *nodeService) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *nodeService) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *nodeService) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).InfoS("NodeGetCapabilities: called", "args", util.SanitizeRequest(req))
	var caps []*csi.NodeServiceCapability
	for _, cap := range nodeCaps {
		c := &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *nodeService) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if id := os.Getenv("CSI_NODE_NAME"); id != "" {
		klog.InfoS("NodeGetInfo: Got CSI_NODE_NAME from env", "CSI_NODE_NAME", id)
		return &csi.NodeGetInfoResponse{NodeId: id}, nil
	}

	klog.InfoS("NodeGetInfo: get node name from metadata")
	region := os.Getenv("AWS_REGION")
	metadata, err := cloud.NewMetadataService(cloud.DefaultEC2MetadataClient, cloud.DefaultKubernetesAPIClient, region)
	if err != nil {
		panic(err)
	}

	return &csi.NodeGetInfoResponse{
		NodeId: metadata.GetInstanceID(),
	}, nil
}

// unmount performs an unmount of the given target path. If forcefulUnmountTimeout
// is configured, it runs the unmount in a goroutine and polls IsLikelyNotMountPoint
// every 5 seconds. If the mount point disappears before the process exits, it
// returns success immediately (the kernel-level work is done even if the umount
// process is still cleaning up). If the timeout elapses and the mount point is
// still present, it retries with umount -f via UnmountWithForce.
// When forcefulUnmountTimeout is zero, it falls back to a plain blocking Unmount.
func (d *nodeService) unmount(target string) error {
	if d.driverOptions.forcefulUnmountTimeout == 0 {
		return d.mounter.Unmount(target)
	}

	const pollInterval = 5 * time.Second

	// Run the normal unmount in the background; it may hang.
	done := make(chan error, 1)
	go func() {
		done <- d.mounter.Unmount(target)
	}()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(d.driverOptions.forcefulUnmountTimeout)
	defer timer.Stop()

	for {
		select {
		case err := <-done:
			// umount process exited on its own.
			return err

		case <-ticker.C:
			// Check whether the mount point has already disappeared.
			notMnt, err := d.mounter.IsLikelyNotMountPoint(target)
			if err == nil && notMnt {
				// Kernel-level unmount is complete. The umount process may still
				// be hanging in userspace cleanup, but the work is done — it is
				// safe to release the lock now.
				klog.V(4).InfoS("NodeUnpublishVolume: mount point gone before umount process exited, treating as success", "target", target)
				return nil
			}

		case <-timer.C:
			// Timeout elapsed and mount point is still present. Fall back to
			// umount -f to force the unmount.
			klog.InfoS("NodeUnpublishVolume: unmount timed out, retrying with forced unmount", "target", target, "timeout", d.driverOptions.forcefulUnmountTimeout)
			forceUnmounter, ok := d.mounter.(mount.MounterForceUnmounter)
			if !ok {
				return fmt.Errorf("unmount of %q timed out after %v and mounter does not support forced unmount", target, d.driverOptions.forcefulUnmountTimeout)
			}
			// UnmountWithForce uses exec.CommandContext internally so it will
			// not block indefinitely.
			return forceUnmounter.UnmountWithForce(target, d.driverOptions.forcefulUnmountTimeout)
		}
	}
}

// isMounted checks if target is mounted. It does NOT return an error if target
// doesn't exist.
func (d *nodeService) isMounted(_ string, target string) (bool, error) {
	/*
		Checking if it's a mount point using IsLikelyNotMountPoint. There are three different return values,
		1. true, err when the directory does not exist or corrupted.
		2. false, nil when the path is already mounted with a device.
		3. true, nil when the path is not mounted with any device.
	*/
	klog.V(4).InfoS(target)
	notMnt, err := d.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		//Checking if the path exists and error is related to Corrupted Mount, in that case, the system could unmount and mount.
		_, pathErr := d.mounter.PathExists(target)
		if pathErr != nil && d.mounter.IsCorruptedMnt(pathErr) {
			klog.V(4).InfoS("NodePublishVolume: Target path is a corrupted mount. Trying to unmount.", "target", target)
			if mntErr := d.mounter.Unmount(target); mntErr != nil {
				return false, status.Errorf(codes.Internal, "Unable to unmount the target %q : %v", target, mntErr)
			}
			//After successful unmount, the device is ready to be mounted.
			return false, nil
		}
		return false, status.Errorf(codes.Internal, "Could not check if %q is a mount point: %v, %v", target, err, pathErr)
	}

	// Do not return os.IsNotExist error. Other errors were handled above.  The
	// Existence of the target should be checked by the caller explicitly and
	// independently because sometimes prior to mount it is expected not to exist
	// (in Windows, the target must NOT exist before a symlink is created at it)
	// and in others it is an error (in Linux, the target mount directory must
	// exist before mount is called on it)
	if err != nil && os.IsNotExist(err) {
		klog.V(5).InfoS("[Debug] NodePublishVolume: Target path does not exist", "target", target)
		return false, nil
	}

	if !notMnt {
		klog.V(4).InfoS("NodePublishVolume: Target path is already mounted", "target", target)
	}

	return !notMnt, nil
}

// Struct for JSON patch operations
type JSONPatch struct {
	OP    string      `json:"op,omitempty"`
	Path  string      `json:"path,omitempty"`
	Value interface{} `json:"value"`
}

// removeTaintInBackground is a goroutine that retries removeNotReadyTaint with exponential backoff
func removeTaintInBackground(k8sClient cloud.KubernetesAPIClient, removalFunc func(cloud.KubernetesAPIClient) error) {
	backoffErr := wait.ExponentialBackoff(taintRemovalBackoff, func() (bool, error) {
		err := removalFunc(k8sClient)
		if err != nil {
			klog.ErrorS(err, "Unexpected failure when attempting to remove node taint(s)")
			return false, nil
		}
		return true, nil
	})

	if backoffErr != nil {
		klog.ErrorS(backoffErr, "Retries exhausted, giving up attempting to remove node taint(s)")
	}
}

// removeNotReadyTaint removes the taint fsx.csi.aws.com/agent-not-ready from the local node
// This taint can be optionally applied by users to prevent startup race conditions such as
// https://github.com/kubernetes/kubernetes/issues/95911
func removeNotReadyTaint(k8sClient cloud.KubernetesAPIClient) error {
	nodeName := os.Getenv("CSI_NODE_NAME")
	if nodeName == "" {
		klog.V(4).InfoS("CSI_NODE_NAME missing, skipping taint removal")
		return nil
	}

	clientset, err := k8sClient()
	if err != nil {
		klog.V(4).InfoS("Failed to setup k8s client, skipping taint removal")
		return nil
	}

	node, err := clientset.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	var taintsToKeep []corev1.Taint
	for _, taint := range node.Spec.Taints {
		if taint.Key != AgentNotReadyNodeTaintKey {
			taintsToKeep = append(taintsToKeep, taint)
		} else {
			klog.V(4).InfoS("Queued taint for removal", "key", taint.Key, "effect", taint.Effect)
		}
	}

	if len(taintsToKeep) == len(node.Spec.Taints) {
		klog.V(4).InfoS("No taints to remove on node, skipping taint removal")
		return nil
	}

	patchRemoveTaints := []JSONPatch{
		{
			OP:    "test",
			Path:  "/spec/taints",
			Value: node.Spec.Taints,
		},
		{
			OP:    "replace",
			Path:  "/spec/taints",
			Value: taintsToKeep,
		},
	}

	patch, err := json.Marshal(patchRemoveTaints)
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Nodes().Patch(context.Background(), nodeName, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	klog.InfoS("Removed taint(s) from local node", "node", nodeName)
	return nil
}
