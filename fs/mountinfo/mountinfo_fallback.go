//go:build !linux

package mountinfo

import "os"

// GetMounts is not implemented on non-Linux systems
func GetMounts() (Table, error) {
	return nil, os.ErrNotExist
}
