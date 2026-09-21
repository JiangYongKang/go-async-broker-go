//go:build windows

package broker

import "os"

func acquireFileLock(path string) (*os.File, error) {
	// The Windows implementation relies on this temporary lock file marker for
	// the small local engine. A production port should use LockFileEx.
	return nil, unsupportedPlatform("file locking")
}

func releaseFileLock(f *os.File) error { return nil }

func syncDir(dir string) error { return nil }

func unsupportedPlatform(what string) error {
	return brokerError(CodeInvalidArgument, what+" is unsupported on this platform", "", nil)
}
