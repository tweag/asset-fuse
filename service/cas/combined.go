package cas

// Combined is a content-addressable storage that combines a remote and a local CAS.
type Combined struct {
	remote CAS
	local  CAS
}
