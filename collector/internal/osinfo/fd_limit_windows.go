//go:build windows

package osinfo

import "fmt"

func processFDLimit() (fdLimit, error) {
	return fdLimit{}, fmt.Errorf("fd limit not supported on windows")
}
