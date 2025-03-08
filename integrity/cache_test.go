package integrity_test

import (
	"testing"

	"github.com/tweag/asset-fuse/integrity"
)

func TestCacheStoreAndLoad(t *testing.T) {
	c := integrity.NewCache()

	hashes, err := integrity.IntegrityFromString(
		"sha256-MgVgyoTIpgiyKd5ahOQPwcqZgp1MPlLDkNuer0+z8pE=",
		"sha384-29vOWFwIfypCjO5d9w75PmSNXxoOZKks8T0MjhVcLvQF4nqUBAvkhN56SO0d7bKK",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, ok := c.FromIntegrity(hashes)
	if ok {
		t.Fatal("cache should be empty")
	}

	// We learned the digest (via the remote-asset api, for example) and now we store it in the cache.
	knownSize := int64(2727)
	c.PutIntegrity(hashes, knownSize)

	expectedDigest := integrity.NewDigest([]byte{
		0x32, 0x05, 0x60, 0xca, 0x84, 0xc8, 0xa6, 0x08, 0xb2, 0x29, 0xde, 0x5a, 0x84, 0xe4, 0x0f, 0xc1,
		0xca, 0x99, 0x82, 0x9d, 0x4c, 0x3e, 0x52, 0xc3, 0x90, 0xdb, 0x9e, 0xaf, 0x4f, 0xb3, 0xf2, 0x91,
	}, knownSize, integrity.SHA256)

	digest, ok := c.FromIntegrity(hashes)
	if !ok {
		t.Fatal("cache should contain the digest")
	}
	if !expectedDigest.Equals(digest, integrity.SHA256) {
		t.Fatalf("expected %v, got %v", expectedDigest, digest)
	}

	// if we use the hash directly, we should get the same result
	var digestArray32 [32]byte
	expectedDigest.CopyHashInto(digestArray32[:], integrity.SHA256)
	digest, ok = c.GetSlice(digestArray32[:], integrity.SHA256.Identifier())
	if !ok {
		t.Fatal("cache should contain the digest")
	}

	// test the same for sha384
	digestArray48 := [48]byte{
		0xdb, 0xdb, 0xce, 0x58, 0x5c, 0x08, 0x7f, 0x2a, 0x42, 0x8c, 0xee, 0x5d, 0xf7, 0x0e, 0xf9, 0x3e,
		0x64, 0x8d, 0x5f, 0x1a, 0x0e, 0x64, 0xa9, 0x2c, 0xf1, 0x3d, 0x0c, 0x8e, 0x15, 0x5c, 0x2e, 0xf4,
		0x05, 0xe2, 0x7a, 0x94, 0x04, 0x0b, 0xe4, 0x84, 0xde, 0x7a, 0x48, 0xed, 0x1d, 0xed, 0xb2, 0x8a,
	}
	expectedDigest = integrity.NewDigest(digestArray48[:], knownSize, integrity.SHA384)
	digest, ok = c.GetSlice(digestArray48[:], integrity.SHA384.Identifier())
	if !ok {
		t.Fatal("cache should contain the digest")
	}

	// check that the identifier is used
	digest, ok = c.GetSlice(digestArray32[:], integrity.SHA384.Identifier())
	if ok {
		t.Fatal("used wrong identifier but got a result")
	}
}
