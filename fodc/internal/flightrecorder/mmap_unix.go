//go:build !windows
// +build !windows

package flightrecorder

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func mmapFile(file *os.File, size int) ([]byte, error) {
	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	return data, nil
}

func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Munmap(data)
}

func msync(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), syscall.MS_SYNC)
	if errno != 0 {
		return fmt.Errorf("msync failed: %w", errno)
	}
	return nil
}

