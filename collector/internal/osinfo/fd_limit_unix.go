//go:build !windows

package osinfo

import "golang.org/x/sys/unix"

func processFDLimit() (fdLimit, error) {
	var limit unix.Rlimit
	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit)
	return fdLimit{Cur: limit.Cur}, err
}
