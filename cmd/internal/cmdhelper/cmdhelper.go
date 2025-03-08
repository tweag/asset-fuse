package cmdhelper

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/internal/logging"
)

func FatalFmt(format string, args ...any) {
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

type OSConfigReader struct {
	ConfigPath string
}

func (r OSConfigReader) Read(config api.GlobalConfig) (api.GlobalConfig, error) {
	file, err := os.Open(r.ConfigPath)
	if err != nil {
		if os.IsNotExist(err) {
			return config, api.ErrConfigNotFound
		}
		return config, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}

	return config, nil
}

func SubstituteHome(p string) string {
	if len(p) == 0 || p[0] != '~' {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return home + p[1:]
}

type FlagPreset uint

const (
	FlagPresetRemote = 1 << iota
	FlagPresetDiskCache
	FlagPresetFUSE
)

type flagConfig struct {
	api.GlobalConfig
	// redefine any bool flags to satisfy flagset.BoolVar
	FUSEDebug bool
	FailReads bool
}

func globalFlags(flagSet *flag.FlagSet, preset FlagPreset) *flagConfig {
	config := &flagConfig{}
	flagSet.StringVar(&config.DigestFunction, "digest_function", "", "Hash function used to compute the digest of a file. It is also used by the remote- and local CAS to reference blobs")
	flagSet.StringVar(&config.ManifestPath, "manifest_path", "", "Path to the manifest file")
	flagSet.StringVar(&config.LogLevel, "log_level", "", `Log level. one of "error", "warning", "basic", "debug"`)

	if preset&FlagPresetDiskCache != 0 {
		flagSet.StringVar(&config.DiskCachePath, "disk_cache", "", "Path to the local (disk) cache directory")
	}
	if preset&FlagPresetRemote != 0 {
		flagSet.StringVar(&config.Remote, "remote", "", "grpc(s) endpoint of the REAPI server")
	}
	if preset&FlagPresetFUSE != 0 {
		flagSet.StringVar(&config.DigestXattrName, "unix_digest_hash_attribute_name", "", `Name of the extended attribute (xattr) used to store the digest of a file. Default: "user.<digest_function>"`)
		flagSet.BoolVar(&config.FailReads, "fail_reads", false, "Emits debug information about the FUSE filesystem")
		flagSet.BoolVar(&config.FUSEDebug, "fuse_debug", false, "Let any read operations on regular files fail with EBADF")
	}
	return config
}

func InjectGlobalFlagsAndConfigure(args []string, flagSet *flag.FlagSet, preset FlagPreset) (api.GlobalConfig, error) {
	var configPath string
	ignoreMissing := true

	if configPathEnv, ok := os.LookupEnv(api.ConfigFileEnv); ok {
		configPath = configPathEnv
		ignoreMissing = false
	}
	flagSet.Func("config", "Path to the config file", func(configPathFlag string) error {
		configPath = configPathFlag
		ignoreMissing = false
		return nil
	})

	flagConfig := globalFlags(flagSet, preset)
	if err := flagSet.Parse(args); err != nil {
		return api.GlobalConfig{}, err
	}
	// fixup any bool vars
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "fuse_debug" {
			flagConfig.GlobalConfig.FUSEDebug = &flagConfig.FUSEDebug
		}
		if f.Name == "fail_reads" {
			flagConfig.GlobalConfig.FailReads = &flagConfig.FailReads
		}
	})

	fileConfig, err := readConfigFileOrDefault(configPath, ignoreMissing)
	if err != nil {
		return api.GlobalConfig{}, err
	}

	config, err := mergeConfigs(fileConfig, flagConfig.GlobalConfig)
	if err != nil {
		return api.GlobalConfig{}, err
	}

	logging.SetLevel(logging.FromString(config.LogLevel))
	return config, config.Validate()
}

func readConfigFileOrDefault(configPath string, ignoreMissing bool) (api.GlobalConfig, error) {
	config := api.DefaultConfig()

	if ignoreMissing && configPath == "" {
		// default config (parse if exists)
		configPath = ".asset-fuse.json"
	}
	configReader := OSConfigReader{ConfigPath: configPath}
	config, err := api.ReadConfig(configReader, config)
	if ignoreMissing && err == api.ErrConfigNotFound {
		return config, nil
	} else if err != nil {
		return api.GlobalConfig{}, fmt.Errorf("reading config from %s: %w", configPath, err)
	}
	return config, nil
}

func mergeConfigs(base, overlay api.GlobalConfig) (api.GlobalConfig, error) {
	overlayJSON, err := json.Marshal(overlay)
	if err != nil {
		return api.GlobalConfig{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(overlayJSON))
	decoder.DisallowUnknownFields()

	merged := base
	err = decoder.Decode(&merged)
	if err != nil {
		return api.GlobalConfig{}, err
	}
	return merged, nil
}
