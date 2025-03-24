package api

import "github.com/tweag/asset-fuse/integrity"

// An asset is a reference to blob of data that can be fetched with a remote asset service,
// and accessed via a content-addressable storage system.
// This type does not include the actual data, but only metadata about it.
type Asset struct {
	URIs       []string
	Integrity  integrity.Integrity
	Qualifiers map[string]string
}

const (
	FSTypeParent = "fuse"
	FSTypeChild  = "asset"
	FSType       = FSTypeParent + "." + FSTypeChild
)
