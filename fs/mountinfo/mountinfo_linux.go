//go:build linux

package mountinfo

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// GetMounts returns a list of mounts from /proc/self/mountinfo
func GetMounts() (Table, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	return parseTable(data)
}

func parseTable(info []byte) (Table, error) {
	table := Table{}
	scanner := bufio.NewScanner(bytes.NewReader(info))
	for scanner.Scan() {
		rawEntry := scanner.Text()
		mountInfo, err := parseMountInfo(rawEntry)
		if err != nil {
			return nil, err
		}
		table = append(table, mountInfo)
	}

	err := scanner.Err()
	return table, err
}

func parseMountInfo(rawEntry string) (MountInfo, error) {
	var err error

	fields := strings.Split(rawEntry, " ")
	mountInfoLength := len(fields)
	if mountInfoLength < 10 {
		return MountInfo{}, fmt.Errorf("mountinfo entry needs to have 10 fields, found %d: %s", len(fields), rawEntry)
	}

	if fields[mountInfoLength-4] != "-" {
		return MountInfo{}, errors.New(`expected separator "-" at the end of optional fields`)
	}

	mount := MountInfo{
		MajorMinorStDev: fields[2],
		Root:            fields[3],
		MountPoint:      fields[4],
		Options:         mountOptions(fields[5]),
		OptionalFields:  nil,
		FSType:          fields[mountInfoLength-3],
		Source:          fields[mountInfoLength-2],
		SuperOptions:    mountOptions(fields[mountInfoLength-1]),
	}

	mount.MountID, err = strconv.Atoi(fields[0])
	if err != nil {
		return MountInfo{}, fmt.Errorf("parsing mount ID %q as integer: %w", mount.MountID, err)
	}
	mount.ParentID, err = strconv.Atoi(fields[1])
	if err != nil {
		return MountInfo{}, fmt.Errorf("parsing parent ID %q as integer: %w", mount.ParentID, err)
	}
	if fields[6] != "" {
		mount.OptionalFields, err = optionalFields(fields[6 : mountInfoLength-4])
		if err != nil {
			return MountInfo{}, fmt.Errorf("parsing optional fields: %w", err)
		}
	}
	return mount, nil
}

func optionalFields(o []string) (map[string]string, error) {
	fields := make(map[string]string)
	for _, field := range o {
		parts := strings.SplitN(field, ":", 2)
		var value string
		if len(parts) == 2 {
			value = parts[1]
		}
		fields[parts[0]] = value
	}
	return fields, nil
}

func mountOptions(mountOptions string) map[string]string {
	optionMap := make(map[string]string)
	optionList := strings.SplitSeq(mountOptions, ",")
	for opt := range optionList {
		parts := strings.Split(opt, "=")
		if len(parts) < 2 {
			key := parts[0]
			optionMap[key] = ""
		} else {
			key, value := parts[0], parts[1]
			optionMap[key] = value
		}
	}
	return optionMap
}
