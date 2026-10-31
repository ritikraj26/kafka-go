package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

func SyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// MakeDirectory syncs newly created entries in their parents as well.
// Configured ancestors such as /tmp may themselves be symlinks.
func MakeDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("not a directory: %s", dir)
		}
		return SyncDirectory(dir)
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return err
	}
	if err := MakeDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	if err := SyncDirectory(dir); err != nil {
		return err
	}
	return SyncDirectory(parent)
}
