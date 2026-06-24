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
	"time"

	"k8s.io/mount-utils"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/cloud"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/driver/internal"
)

// fakeForceMounter adds UnmountWithForce to mount.FakeMounter, which does not
// implement mount.MounterForceUnmounter upstream. Without it, NodeMounter's
// delegating UnmountWithForce would always fail the type assertion in tests and
// the forced-unmount path would be untestable.
type fakeForceMounter struct {
	*mount.FakeMounter
}

func (m *fakeForceMounter) UnmountWithForce(target string, _ time.Duration) error {
	return m.Unmount(target)
}

func NewFakeMounter() Mounter {
	return &NodeMounter{
		Interface: &fakeForceMounter{
			FakeMounter: &mount.FakeMounter{
				MountPoints: []mount.MountPoint{},
			},
		},
	}
}

// NewFakeDriver creates a new mock driver used for testing
func NewFakeDriver(endpoint string) *Driver {
	driverOptions := DriverOptions{
		endpoint: endpoint,
		mode:     AllMode,
	}

	driver := &Driver{
		options: &driverOptions,
		controllerService: controllerService{
			cloud:         cloud.NewFakeCloudProvider(),
			inFlight:      internal.NewInFlight(),
			driverOptions: &driverOptions,
		},
		nodeService: nodeService{
			mounter:       NewFakeMounter(),
			inFlight:      internal.NewInFlight(),
			driverOptions: &DriverOptions{forcefulUnmountTimeout: -1},
		},
	}
	return driver
}
