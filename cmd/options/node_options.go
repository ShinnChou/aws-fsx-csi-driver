/*
Copyright 2020 The Kubernetes Authors.

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

package options

import (
	"time"

	flag "github.com/spf13/pflag"
)

// NodeOptions contains options and configuration settings for the node service.
type NodeOptions struct {
	// ForcefulUnmountTimeout is the duration after which a hanging unmount will be
	// retried with umount -f, while periodically checking if the mount point has
	// already disappeared. Set to 0 to disable (default, preserves existing behavior).
	ForcefulUnmountTimeout time.Duration
}

func (o *NodeOptions) AddFlags(fs *flag.FlagSet) {
	fs.DurationVar(&o.ForcefulUnmountTimeout, "forceful-unmount-timeout", 0,
		"Duration to wait for a normal unmount before retrying with umount -f. "+
			"During this period the mount point is polled every 5s; if it disappears "+
			"the lock is released immediately even if the umount process has not exited. "+
			"Set to 0 to disable (default).")
}
