//go:build !windows

package specfts

import "os"

func replaceFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
