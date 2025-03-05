package fs

import (
	"syscall"
)

// modeRegularReadonly is the mode for regular files that are read-only.
// This sets the r bit for all users.
const modeRegularReadonly = syscall.S_IFREG | 0o444

// modeDirReadonly is the mode for directories that are read-only
// This sets the r and x bits for all users,
// which is needed to "cd" into the directory and list its contents.
const modeDirReadonly = syscall.S_IFDIR | 0o555
