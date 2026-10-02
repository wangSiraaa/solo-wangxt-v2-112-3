//go:build !linux

package backup

import (
	"os"
)

func openExclusiveFile(path string, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode.Perm())
}

func chown(path string, uid, gid int) {}

func lchown(path string, uid, gid int) {}
