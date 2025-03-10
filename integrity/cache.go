package integrity

import (
	"hash/maphash"
	"sync"
)

// checksumCache contains a map of checksums to digests.
// The key can be of any hash function, as long as it can be padded to 64 bytes.
// One byte is reserved for the identifier of the checksum.
// The value is a digest (for the main digest function "--digest_function").
type ChecksumCache struct {
	shards [shardCount]map[uint64]Digest
	muxs   [shardCount]sync.RWMutex
	seed   maphash.Seed
}

func NewCache() *ChecksumCache {
	cache := &ChecksumCache{
		shards: [shardCount]map[uint64]Digest{},
		seed:   maphash.MakeSeed(),
	}
	for i := range cache.shards {
		cache.shards[i] = make(map[uint64]Digest)
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

	var key maphash.Hash
	key.SetSeed(c.seed)
	key.Write(hash)
	key.WriteByte(identifier)

	digest, ok := c.shards[shard][key.Sum64()]
	return digest, ok
}

func (c *ChecksumCache) PutSlice(hash []byte, identifier byte, digest Digest) {
	if len(hash) == 0 {
		return
	}
	shard := hash[0] & shardMask
	c.muxs[shard].Lock()
	defer c.muxs[shard].Unlock()

	var key maphash.Hash
	key.SetSeed(c.seed)
	key.Write(hash)
	key.WriteByte(identifier)

	c.shards[shard][key.Sum64()] = digest
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

func (c *ChecksumCache) PutIntegrity(integrity Integrity, digest Digest) {
	for checksum := range integrity.Items() {
		c.PutSlice(checksum.Hash, checksum.Algorithm.Identifier(), digest)
	}
}

const (
	shardCount = 2 << 7
	shardMask  = shardCount - 1
)
