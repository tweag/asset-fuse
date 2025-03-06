package integrity

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"strings"
)

// Digest represents the digest of a blob in the
// Content Addressable Storage (CAS), as specified in
// the remote execution API, including the hash and content size (in bytes).
// Unlike the remote execution API, the hash is encoded as a byte array.
type Digest struct {
	// Inlined array of bytes representing the hash.
	// This uses the theoretical maximum size of a hash (64 bytes).
	// All public methods correctly handle the actual hash size.
	// The contents of the unused bytes are unspecified and must be ignored.
	hash [64]byte
	// Size of the content in bytes.
	SizeBytes int64
}

func NewDigest(hash []byte, sizeBytes int64, algorithm Algorithm) Digest {
	if len(hash) != algorithm.SizeBytes() {
		// TODO: decide if we should panic here or return an error
		panic("hash length does not match algorithm size")
	}
	out := Digest{SizeBytes: sizeBytes}
	copy(out.hash[:], hash)
	return out
}

func DigestFromHex(hexDigest string, sizeBytes int64, algorithm Algorithm) (Digest, error) {
	// This function may be used to parse digests from the remote execution API.
	// In the remote execution API, the hash is encoded as a lowercase hexadecimal string.
	// The spec mentions that the hash may be padded with leading zeros up to the hash function length.
	// In practice, any implementation for supported hash functions should have a fixed length (32, 48, or 64 bytes).
	hash, err := hex.DecodeString(hexDigest)
	if err != nil {
		return Digest{}, fmt.Errorf("failed to decode hex digest %q: %w", hexDigest, err)
	}
	if len(hash) != algorithm.SizeBytes() {
		return Digest{}, fmt.Errorf("unexpected hash size in hex digest %q: got %d, want %d", hexDigest, len(hash), algorithm.SizeBytes())
	}
	return NewDigest(hash, sizeBytes, algorithm), nil
}

func (d Digest) Equals(other Digest, algorithm Algorithm) bool {
	if d.Uninitialized() || other.Uninitialized() {
		// for safety, uninitialized digests are never equal to anything
		return false
	}
	if d.SizeBytes != other.SizeBytes {
		return false
	}
	switch algorithm {
	case SHA256:
		return bytes.Equal(d.hash[:32], other.hash[:32])
	case SHA384:
		return bytes.Equal(d.hash[:48], other.hash[:48])
	case SHA512:
		return bytes.Equal(d.hash[:64], other.hash[:64])
	case Blake3:
		return bytes.Equal(d.hash[:32], other.hash[:32])
	}
	// Should be unreachable.
	panic("unsupported algorithm")
}

func (d Digest) ZeroSized(algorithm Algorithm) bool {
	if d.SizeBytes != 0 {
		return false
	}
	switch algorithm {
	case SHA256:
		return bytes.Equal(d.hash[:], zeroSizedChecksumSHA256[:])
	case SHA384:
		return bytes.Equal(d.hash[:], zeroSizedChecksumSHA384[:])
	case SHA512:
		return bytes.Equal(d.hash[:], zeroSizedChecksumSHA512[:])
	case Blake3:
		return bytes.Equal(d.hash[:], zeroSizedChecksumBlake3[:])
	}
	// Should be unreachable.
	return false
}

func (d Digest) Uninitialized() bool {
	return d.SizeBytes == 0 && d.hash == [64]byte{}
}

// CopyHashInto copies the hash into the destination buffer.
// The destination buffer must be at least the size of the hash.
func (d Digest) CopyHashInto(dest []byte, algorithm Algorithm) error {
	sz := algorithm.SizeBytes()
	if len(dest) < sz {
		return fmt.Errorf("destination buffer is too small: got %d, want %d", len(dest), sz)
	}
	copy(dest, d.hash[:sz])
	return nil
}

func (d Digest) Hex(algorithm Algorithm) string {
	sz := algorithm.SizeBytes()
	return hex.EncodeToString(d.hash[:sz])
}

// Checksum represents a single checksum of an artifact for a specific algorithm.
// It doesn't contain the size of the contents.
type Checksum struct {
	Algorithm Algorithm
	Hash      []byte
}

func ChecksumFromSRI(integrity string) (Checksum, error) {
	var checksum Checksum
	var expectedByteSize int
	switch integrity[:7] {
	case "sha256-":
		checksum.Algorithm = SHA256
		expectedByteSize = 32
	case "sha384-":
		checksum.Algorithm = SHA384
		expectedByteSize = 48
	case "sha512-":
		checksum.Algorithm = SHA512
		expectedByteSize = 64
	case "blake3-":
		checksum.Algorithm = Blake3
		expectedByteSize = 32
	default:
		return checksum, fmt.Errorf("unsupported algorithm in sri: %s", integrity)
	}

	hash, err := base64.StdEncoding.DecodeString(integrity[7:])
	if err != nil {
		return checksum, fmt.Errorf("failed to decode sri hash from base64 in %q: %w", integrity, err)
	}
	if len(hash) != expectedByteSize {
		return checksum, fmt.Errorf("unexpected hash size in sri %q: got %d, want %d", integrity, len(hash), expectedByteSize)
	}
	checksum.Hash = hash
	return checksum, nil
}

func ChecksumFromDigest(digest Digest, algorithm Algorithm) Checksum {
	return Checksum{Algorithm: algorithm, Hash: digest.hash[:algorithm.SizeBytes()]}
}

func (c Checksum) ToSRI() string {
	return fmt.Sprintf("%s-%s", c.Algorithm.String(), base64.StdEncoding.EncodeToString(c.Hash))
}

func (c Checksum) Equals(other Checksum) bool {
	return c.Algorithm == other.Algorithm && len(c.Hash) > 0 && len(other.Hash) > 0 && bytes.Equal(c.Hash, other.Hash)
}

// Empty returns true if the checksum is empty.
func (c Checksum) Empty() bool {
	return len(c.Hash) == 0
}

// ZeroSized returns true if the checksum is well-known
// to represent a zero-sized file.
func (c Checksum) ZeroSized() bool {
	switch c.Algorithm {
	case SHA256:
		return bytes.Equal(c.Hash, zeroSizedChecksumSHA256[:])
	case SHA384:
		return bytes.Equal(c.Hash, zeroSizedChecksumSHA384[:])
	case SHA512:
		return bytes.Equal(c.Hash, zeroSizedChecksumSHA512[:])
	case Blake3:
		return bytes.Equal(c.Hash, zeroSizedChecksumBlake3[:])
	}
	return false
}

// Integrity represents the integrity of an artifact, including checksums for
// multiple algorithms.
// This representation is not space-efficient, but it doesn't require
// additional allocations for each checksum.
// If the number of supported algorithms increases, this representation
// should be changed to a map.
type Integrity struct {
	sha256 Checksum
	sha384 Checksum
	sha512 Checksum
	blake3 Checksum
}

func (i Integrity) Empty() bool {
	return i.sha256.Hash == nil && i.sha384.Hash == nil && i.sha512.Hash == nil && i.blake3.Hash == nil
}

func (i Integrity) Items() iter.Seq[Checksum] {
	return func(yield func(Checksum) bool) {
		if checksum, ok := i.ChecksumForAlgorithm(SHA256); ok {
			if !yield(checksum) {
				return
			}
		}
		if checksum, ok := i.ChecksumForAlgorithm(SHA384); ok {
			if !yield(checksum) {
				return
			}
		}
		if checksum, ok := i.ChecksumForAlgorithm(SHA512); ok {
			if !yield(checksum) {
				return
			}
		}
		if checksum, ok := i.ChecksumForAlgorithm(Blake3); ok {
			if !yield(checksum) {
				return
			}
		}
	}
}

// Equivalent returns true if the two Integrity objects are equivalent.
// This means: for each algorithm that has a checksum in both objects,
// the checksums are equal.
// We also require that at least one checksum is present in both objects.
// Additionally, any object with no checksums is considered unequal to any other object.
func (i Integrity) Equivalent(other Integrity) bool {
	if i.Empty() || other.Empty() {
		return false
	}
	var matchingChecksums int
	if i.sha256.Hash != nil && other.sha256.Hash != nil {
		matchingChecksums++
		if !bytes.Equal(i.sha256.Hash, other.sha256.Hash) {
			return false
		}
	}
	if i.sha384.Hash != nil && other.sha384.Hash != nil {
		matchingChecksums++
		if !bytes.Equal(i.sha384.Hash, other.sha384.Hash) {
			return false
		}
	}
	if i.sha512.Hash != nil && other.sha512.Hash != nil && !bytes.Equal(i.sha512.Hash, other.sha512.Hash) {
		matchingChecksums++
		if !bytes.Equal(i.sha512.Hash, other.sha512.Hash) {
			return false
		}
	}
	if i.blake3.Hash != nil && other.blake3.Hash != nil && !bytes.Equal(i.blake3.Hash, other.blake3.Hash) {
		matchingChecksums++
		if !bytes.Equal(i.blake3.Hash, other.blake3.Hash) {
			return false
		}
	}
	return matchingChecksums > 0
}

func IntegrityFromString(integrity ...string) (Integrity, error) {
	if len(integrity) == 0 {
		return Integrity{}, nil
	}
	out := Integrity{}
	for i, sri := range integrity {
		c, err := ChecksumFromSRI(sri)
		if err != nil {
			return Integrity{}, fmt.Errorf("parsing integrity string %d: %w", i, err)
		}
		switch c.Algorithm {
		case SHA256:
			if out.sha256.Hash != nil {
				return Integrity{}, errors.New("duplicate sha256 checksums in integrity strings")
			}
			out.sha256 = c
		case SHA384:
			if out.sha384.Hash != nil {
				return Integrity{}, errors.New("duplicate sha384 checksums in integrity strings")
			}
			out.sha384 = c
		case SHA512:
			if out.sha512.Hash != nil {
				return Integrity{}, errors.New("duplicate sha512 checksums in integrity strings")
			}
			out.sha512 = c
		case Blake3:
			if out.blake3.Hash != nil {
				return Integrity{}, errors.New("duplicate blake3 checksums in integrity strings")
			}
			out.blake3 = c
		default:
			return Integrity{}, fmt.Errorf("unsupported algorithm in integrity string: %s", c.Algorithm)
		}
	}
	return out, nil
}

func IntegrityFromChecksums(checksums ...Checksum) Integrity {
	if len(checksums) == 0 {
		return Integrity{}
	}
	i := Integrity{}
	for _, c := range checksums {
		switch c.Algorithm {
		case SHA256:
			i.sha256 = c
		case SHA384:
			i.sha384 = c
		case SHA512:
			i.sha512 = c
		case Blake3:
			i.blake3 = c
		}
	}
	return i
}

func (i Integrity) ChecksumForAlgorithm(alg Algorithm) (Checksum, bool) {
	switch alg {
	case SHA256:
		return i.sha256, i.sha256.Hash != nil
	case SHA384:
		return i.sha384, i.sha384.Hash != nil
	case SHA512:
		return i.sha512, i.sha512.Hash != nil
	case Blake3:
		return i.blake3, i.blake3.Hash != nil
	}
	return Checksum{}, false
}

// BestSingleChecksum returns the best single checksum (with preferrence for the given algorithm).
// Alternatively, other algorithms are allowed.
func (i Integrity) BestSingleChecksum(alg Algorithm) (Checksum, bool) {
	// Always prefer the algorithm used for digests
	if c, ok := i.ChecksumForAlgorithm(alg); ok {
		return c, true
	}

	// Otherwise, we prefer SHA256 (most widely supported)
	if c, ok := i.ChecksumForAlgorithm(SHA256); ok {
		return c, true
	}

	// Otherwise, we try the fastest (Blake3)
	if c, ok := i.ChecksumForAlgorithm(Blake3); ok {
		return c, true
	}

	// Otherwise, we try the most secure (SHA512)
	if c, ok := i.ChecksumForAlgorithm(SHA512); ok {
		return c, true
	}

	// Otherwise, we try the least used (SHA384)
	if c, ok := i.ChecksumForAlgorithm(SHA384); ok {
		return c, true
	}
	return Checksum{}, false
}

type Algorithm struct{ name string }

func (a Algorithm) String() string { return a.name }

func AlgorithmFromString(name string) (Algorithm, bool) {
	name = strings.ToLower(name)
	switch name {
	case "sha256":
		return SHA256, true
	case "sha384":
		return SHA384, true
	case "sha512":
		return SHA512, true
	case "blake3":
		return Blake3, true
	}
	return Algorithm{}, false
}

func (a Algorithm) SizeBytes() int {
	switch a {
	case SHA256:
		return 32
	case SHA384:
		return 48
	case SHA512:
		return 64
	case Blake3:
		return 32
	}
	// Should be unreachable.
	panic("unsupported algorithm")
}

var (
	SHA256          Algorithm = Algorithm{"sha256"}
	SHA384          Algorithm = Algorithm{"sha384"}
	SHA512          Algorithm = Algorithm{"sha512"}
	Blake3          Algorithm = Algorithm{"blake3"}
	KnownAlgorithms           = []Algorithm{SHA256, SHA384, SHA512, Blake3}
)

var (
	// zeroSizedChecksumSHA256 is sha256 of an empty file.
	// Hex: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	zeroSizedChecksumSHA256 = [32]byte{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	// zeroSizedChecksumSHA384 is sha384 of an empty file.
	// Hex: 38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b
	zeroSizedChecksumSHA384 = [48]byte{
		0x38, 0xb0, 0x60, 0xa7, 0x51, 0xac, 0x96, 0x38, 0x4c, 0xd9, 0x32, 0x7e, 0xb1, 0xb1, 0xe3, 0x6a,
		0x21, 0xfd, 0xb7, 0x11, 0x14, 0xbe, 0x07, 0x43, 0x4c, 0x0c, 0xc7, 0xbf, 0x63, 0xf6, 0xe1, 0xda,
		0x27, 0x4e, 0xde, 0xbf, 0xe7, 0x6f, 0x65, 0xfb, 0xd5, 0x1a, 0xd2, 0xf1, 0x48, 0x98, 0xb9, 0x5b,
	}

	// zeroSizedChecksumSHA512 is sha512 of an empty file.
	// Hex: cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e
	zeroSizedChecksumSHA512 = [64]byte{
		0xcf, 0x83, 0xe1, 0x35, 0x7e, 0xef, 0xb8, 0xbd, 0xf1, 0x54, 0x28, 0x50, 0xd6, 0x6d, 0x80, 0x07,
		0xd6, 0x20, 0xe4, 0x05, 0x0b, 0x57, 0x15, 0xdc, 0x83, 0xf4, 0xa9, 0x21, 0xd3, 0x6c, 0xe9, 0xce,
		0x47, 0xd0, 0xd1, 0x3c, 0x5d, 0x85, 0xf2, 0xb0, 0xff, 0x83, 0x18, 0xd2, 0x87, 0x7e, 0xec, 0x2f,
		0x63, 0xb9, 0x31, 0xbd, 0x47, 0x41, 0x7a, 0x81, 0xa5, 0x38, 0x32, 0x7a, 0xf9, 0x27, 0xda, 0x3e,
	}

	// zeroSizedChecksumBlake3 is blake3 of an empty file.
	// Hex: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
	zeroSizedChecksumBlake3 = [32]byte{
		0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
		0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
	}
)
