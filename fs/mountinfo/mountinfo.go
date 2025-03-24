package mountinfo

import (
	"path/filepath"

	"github.com/tweag/asset-fuse/api"
)

type Table []MountInfo

func (t Table) MountPoint(mountPoint string) (MountInfo, bool) {
	mountPoint, err := filepath.Abs(mountPoint)
	if err != nil {
		// This should never happen
		panic(err)
	}
	for _, mount := range t {
		if mount.MountPoint == mountPoint {
			return mount, true
		}
	}
	return MountInfo{}, false
}

type MountInfo struct {
	// A unique ID for the mount
	MountID int
	// The ID of the parent mount
	ParentID int
	// The value of `st_dev` for the files on this filesystem
	MajorMinorStDev string
	// The pathname of the directory in the filesystem
	// which forms the root for this mount
	Root string
	// The pathname of the mount point relative to the process's root directory.
	MountPoint string
	// Mount options
	Options map[string]string
	// Zero or more optional fields
	OptionalFields map[string]string
	// The Filesystem type
	FSType string
	// Filesystem specific information or "none"
	Source string
	// Per-superblock options
	SuperOptions map[string]string
}

func (i MountInfo) IsAssetFuse() bool {
	return i.FSType == api.FSType
}
