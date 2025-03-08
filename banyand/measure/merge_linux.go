//go:build linux

package measure

import (
	"golang.org/x/sys/unix"
	"os"
)

func applyFadvise(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.POSIX_FADV_DONTNEED)
}
