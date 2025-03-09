package api

import (
	"errors"
	"slices"
	"strings"
)

// GlobalConfig is the configuration for the asset-fuse filesystem.
// It can be read from a JSON file or passed as command-line flags.
// This configuration is shared by all subcommands.
type GlobalConfig struct {
	// DigestFunction is the hash function used to compute the digest of a file.
	// It is also used by the remote- and local CAS to reference blobs.
	DigestFunction string `json:"digest_function,omitempty"`
	// Name of the extended attribute (xattr) used to store the digest of a file.
	DigestXattrName string `json:"unix_digest_hash_attribute_name,omitempty"`
	// The path to the manifest file.
	ManifestPath string `json:"manifest,omitempty"`
	// The path to the local (disk) cache directory.
	DiskCachePath string `json:"disk_cache,omitempty"`
	// The grpc(s) endpoint of the REAPI server,
	// providing access to the remote content-addressable storage
	// and the remote asset service.
	// Example: "grpcs://remote.buildbuddy.io"
	// Example: "grpc://localhost:8980" (for unencrypted connections - not recommended)
	Remote string `json:"remote,omitempty"`
	// Let any read operations on regular files fail with EBADF.
	// This is useful to test if prefetching and xattr optimizations are working with Buck2 and Bazel:
	// When remote execution is used and the remote asset service is available,
	// Buck2 and Bazel should read digests via xattr and never try to get file contents locally.
	// Instead, they should always use the remote asset service to fetch the file contents directly
	// from the internet into the remote CAS.
	FailReads *bool `json:"fail_reads,omitempty"`
	// Emits debug information about the FUSE filesystem.
	FUSEDebug *bool `json:"fuse_debug,omitempty"`
	// Log level. One of "error", "warning", "basic", "debug".
	// Note that some messages are always printed, regardless of the log level (e.g. errors).
	// Default: "info"
	LogLevel string `json:"log_level,omitempty"`
}

func (c GlobalConfig) Validate() error {
	issues := []string{}
	switch c.DigestFunction {
	case "sha256", "sha384", "sha512", "blake3": // allowed
	case "":
		issues = append(issues, `digest_function must be provided`)
	default:
		issues = append(issues, `digest_function must be one of "sha256", "sha384", "sha512", "blake3"`)
	}
	if c.ManifestPath == "" {
		issues = append(issues, `manifest must be provided`)
	}
	if c.DiskCachePath == "" {
		issues = append(issues, `disk_cache must be provided`)
	}
	if c.Remote == "" {
		// TODO: should we allow empty remote?
		issues = append(issues, `remote must be provided`)
	} else if !slices.Contains([]string{"grpcs", "grpc"}, strings.Split(c.Remote, "://")[0]) {
		issues = append(issues, `remote must start with "grpcs://" or "grpc://"`)
	}
	switch c.LogLevel {
	case "error", "warning", "basic", "debug": // allowed
	default:
		issues = append(issues, `log_level must be one of "error", "warning", "basic", "debug"`)
	}

	if len(issues) > 0 {
		return errors.New("config validation failed: \n  " + strings.Join(issues, "\n  "))
	}
	return nil
}

func (c GlobalConfig) FUSEDebugEnable() bool {
	return c.FUSEDebug != nil && *c.FUSEDebug
}

type ConfigReader interface {
	Read(baseConfig GlobalConfig) (GlobalConfig, error)
}

func ReadConfig(reader ConfigReader, config GlobalConfig) (GlobalConfig, error) {
	return reader.Read(config)
}

func DefaultConfig() GlobalConfig {
	return GlobalConfig{
		DigestFunction:  "sha256",
		DigestXattrName: "", // disable custom name by default
		ManifestPath:    "manifest.json",
		DiskCachePath:   "~/.cache/asset-fuse",
		// TODO: remove this default value
		// Pointing at a SaaS service is not a good default.
		Remote:    "grpcs://remote.buildbuddy.io",
		FailReads: nil,
		FUSEDebug: nil,
		LogLevel:  "info",
	}
}
