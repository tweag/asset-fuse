package fs

// flags in file.f_mode.  Note that FMODE_READ and FMODE_WRITE must correspond
// to O_WRONLY and O_RDWR via the strange trick in __dentry_open()
const (
	// file is open for reading
	FMODE_READ = 1 << 0
	// file is open for writing
	FMODE_WRITE = 1 << 1
	// file is seekable
	FMODE_LSEEK = 1 << 2
	// file can be accessed using pread
	FMODE_PREAD = 1 << 3
	// file can be accessed using pwrite
	FMODE_PWRITE = 1 << 4
	// file is opened for execution with sys_execve / sys_uselib
	FMODE_EXEC = 1 << 5
	// file writes are restricted (block device specific)
	FMODE_WRITE_RESTRICTED = 1 << 6
	// file supports atomic writes
	FMODE_CAN_ATOMIC_WRITE = 1 << 7
	// 32bit hashes as llseek() offset (for directories)
	FMODE_32BITHASH = 1 << 9
	// 64bit hashes as llseek() offset (for directories)
	FMODE_64BITHASH = 1 << 10
	// don't update ctime and mtime
	FMODE_NOCMTIME = 1 << 11
	// expect random access pattern
	FMODE_RANDOM = 1 << 12
	// file is opened with O_PATH; almost nothing can be done with it
	FMODE_PATH = 1 << 14
	// file needs atomic accesses to f_pos
	FMODE_ATOMIC_POS = 1 << 15
	// write access to underlying fs
	FMODE_WRITER = 1 << 16
	// has read method(s)
	FMODE_CAN_READ = 1 << 17
	// has write method(s)
	FMODE_CAN_WRITE = 1 << 18
	FMODE_OPENED    = 1 << 19
	FMODE_CREATED   = 1 << 20
	// file is stream-like
	FMODE_STREAM = 1 << 21
	// file supports DIRECT IO
	FMODE_CAN_ODIRECT = 1 << 22
	FMODE_NOREUSE     = 1 << 23
	// file is embedded in backing_file object
	FMODE_BACKING = 1 << 25
	// file was opened by fanotify and shouldn't generate fanotify events
	FMODE_NONOTIFY = 1 << 26
	// file is capable of returning -EAGAIN if I/O will block
	FMODE_NOWAIT = 1 << 27
	// file represents mount that needs unmounting
	FMODE_NEED_UNMOUNT = 1 << 28
	// file does not contribute to nr_files count
	FMODE_NOACCOUNT = 1 << 29
)

const FMODE_ACCESS = FMODE_READ | FMODE_WRITE | FMODE_EXEC
