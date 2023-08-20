package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// ObjectTypes are subdirs that are used for object storage
var ObjectTypes = []string{"data", "index", "keys", "locks", "snapshots"}

// Blob represents a single blob, its name and its size.
type Blob struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type Filesystem interface {
	CreateRepo(path string, mode os.FileMode) error

	GetConfig(path string) ([]byte, error)
	CheckConfig(path string) (size int64, err error)
	SaveConfig(path string, cfg []byte) error
	DeleteConfig(path string) error

	ListBlobs(path string) ([]Blob, error)

	GetBlob(path string) (io.Reader, error)
	SaveBlob(path string)
	CheckBlob(path string) (size int64, err error)
	DeleteBlob(path string, needSize bool) (size int64, err error)
}

type DiskFilesystem struct{}

func syncFile(f *os.File) (bool, error) {
	err := f.Sync()
	// Ignore error if filesystem does not support fsync.
	syncNotSup := err != nil && (errors.Is(err, syscall.ENOTSUP) || isMacENOTTY(err))
	if syncNotSup {
		err = nil
	}
	return syncNotSup, err
}

func syncDir(dirname string) error {
	if runtime.GOOS == "windows" {
		// syncing a directory is not possible on windows
		return nil
	}

	dir, err := os.Open(dirname)
	if err != nil {
		return err
	}
	err = dir.Sync()
	// Ignore error if filesystem does not support fsync.
	if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EINVAL) {
		err = nil
	}
	if err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func (DiskFilesystem) CreateRepo(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}

	for _, d := range ObjectTypes {
		if err := os.Mkdir(filepath.Join(path, d), mode); err != nil && !os.IsExist(err) {
			return err
		}
	}

	for i := 0; i < 256; i++ {
		dirPath := filepath.Join(path, "data", fmt.Sprintf("%02x", i))
		if err := os.Mkdir(dirPath, mode); err != nil && !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func (DiskFilesystem) CheckBlob(path string) (size int64, err error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (DiskFilesystem) DeleteBlob(path string, needSize bool) (size int64, err error) {
	if needSize {
		stat, err := os.Stat(path)
		if err == nil {
			size = stat.Size()
		}
	}

	if err = os.Remove(path); err != nil {
		// ignore not exist errors to make deleting idempotent, which is
		// necessary to properly handle request retries
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	}
	return size, err
}
