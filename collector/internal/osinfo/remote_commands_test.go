package osinfo

import (
	"strings"
	"testing"
)

func TestRemoteFilesystemCommandUsesUsePercentColumn(t *testing.T) {
	if !strings.Contains(remoteFilesystemCommand, "$6") {
		t.Fatalf("expected remoteFilesystemCommand to read df use percent column: %s", remoteFilesystemCommand)
	}
}
