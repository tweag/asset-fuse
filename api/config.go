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
	// The path to the manifest file.
	ManifestPath string `json:"manifest_path,omitempty"`
	// The path to the local (disk) cache directory.
	DiskCachePath string `json:"disk_cache,omitempty"`
	// The grpc(s) endpoint of the REAPI server,
	// providing access to the remote content-addressable storage
	// and the remote asset service.
	// Example: "grpcs://remote.buildbuddy.io"
	// Example: "grpc://localhost:8980" (for unencrypted connections - not recommended)
	Remote string `json:"remote,omitempty"`
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
		issues = append(issues, `manifest_path must be provided`)
	}
	if c.DiskCachePath == "" {
		issues = append(issues, `disk_cache must be provided`)
	}
	if c.Remote == "" {
		// TODO: should we allow empty remote?
		issues = append(issues, `remote must be provided`)
	}
	if !slices.Contains([]string{"grpcs", "grpc"}, strings.Split(c.Remote, "://")[0]) {
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
		DigestFunction: "sha256",
		ManifestPath:   "manifest.json",
		DiskCachePath:  "~/.cache/asset-fuse",
		// TODO: remove this default value
		// Pointing at a SaaS service is not a good default.
		Remote:    "grpcs://remote.buildbuddy.io",
		FUSEDebug: nil,
		LogLevel:  "info",
	}
}
