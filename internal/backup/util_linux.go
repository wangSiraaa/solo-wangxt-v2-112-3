//go:build linux

package backup

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// openExclusiveFile creates a new regular file that must not already exist.
// O_NOFOLLOW blocks the classic escape: if an attacker pre-creates a symlink
// at the restore path pointing outside the root, this open fails instead of
// writing through the link.
func openExclusiveFile(pathname string, mode os.FileMode) (*os.File, error) {
	fd, err := syscall.Open(pathname, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: pathname, Err: err}
	}
	return os.NewFile(uintptr(fd), pathname), nil
}

func chown(path string, uid, gid int) {
	_ = syscall.Chown(path, uid, gid) // best effort; non-root restores skip ownership
}

// lchown sets ownership of the link itself, never of its target.
func lchown(path string, uid, gid int) {
	_ = unix.Fchownat(unix.AT_FDCWD, path, uid, gid, unix.AT_SYMLINK_NOFOLLOW)
}
