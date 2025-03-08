package integrity

import (
	"sync"
)

// checksumCache contains a map of checksums to digests.
// The key can be of any hash function, as long as it can be padded to 64 bytes.
// One byte is reserved for the identifier of the checksum.
// The value is a digest (for the main digest function "--digest_function").
type ChecksumCache struct {
	shards [shardCount]map[[65]byte]Digest
	muxs   [shardCount]sync.RWMutex
}

func NewCache() *ChecksumCache {
	cache := &ChecksumCache{shards: [shardCount]map[[65]byte]Digest{}}
	for i := range cache.shards {
		cache.shards[i] = make(map[[65]byte]Digest)
	}
	return cache
}

func (c *ChecksumCache) GetSlice(hash []byte, identifier byte) (Digest, bool) {
	if len(hash) == 0 {
		return Digest{}, false
	}
	shard := hash[0] & shardMask
	c.muxs[shard].RLock()
	defer c.muxs[shard].RUnlock()

	var key [65]byte
	copy(key[:64], hash)
	key[64] = identifier

	digest, ok := c.shards[shard][key]
	return digest, ok
}

func (c *ChecksumCache) PutSlice(hash []byte, identifier byte, digest Digest) {
	if len(hash) == 0 {
		return
	}
	shard := hash[0] & shardMask
	c.muxs[shard].Lock()
	defer c.muxs[shard].Unlock()

	var key [65]byte
	copy(key[:64], hash)
	key[64] = identifier

	c.shards[shard][key] = digest
}

func (c *ChecksumCache) FromIntegrity(integrity Integrity) (Digest, bool) {
	for checksum := range integrity.Items() {
		digest, ok := c.GetSlice(checksum.Hash, checksum.Algorithm.Identifier())
		if ok {
			return digest, true
		}
	}
	return Digest{}, false
}

func (c *ChecksumCache) FromChecksum(checksum Checksum) (Digest, bool) {
	return c.GetSlice(checksum.Hash, checksum.Algorithm.Identifier())
}

func (c *ChecksumCache) PutIntegrity(integrity Integrity, sizeBytes int64) {
	for checksum := range integrity.Items() {
		c.PutSlice(checksum.Hash, checksum.Algorithm.Identifier(), NewDigest(checksum.Hash, sizeBytes, checksum.Algorithm))
	}
}

const (
	shardCount = 2 << 7
	shardMask  = shardCount - 1
)
