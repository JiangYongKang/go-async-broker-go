//go:build !windows

package broker

import (
	"os"
	"syscall"
)

func acquireFileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, brokerError(CodeCorruptStorage, "open broker lock file", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, brokerError(CodeLocked, "another broker process holds the storage lock", path, err)
	}
	return f, nil
}

func releaseFileLock(f *os.File) error {
	if f == nil {
		return nil
	}
	lockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	if lockErr != nil {
		return brokerError(CodeCorruptStorage, "release broker lock file", f.Name(), lockErr)
	}
	if closeErr != nil {
		return brokerError(CodeCorruptStorage, "close broker lock file", f.Name(), closeErr)
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return brokerError(CodeCorruptStorage, "open directory for fsync", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return brokerError(CodeCorruptStorage, "fsync directory", dir, err)
	}
	return nil
}
