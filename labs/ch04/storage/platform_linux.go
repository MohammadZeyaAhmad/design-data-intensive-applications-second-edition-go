package storage

import (
	"fmt"
	"syscall"
)

func filesystemInfo(dir string) map[string]any {
	var s syscall.Statfs_t
	if e := syscall.Statfs(dir, &s); e != nil {
		return map[string]any{"error": "filesystem metadata unavailable"}
	}
	name := "unknown"
	switch uint64(s.Type) {
	case 0x1021994:
		name = "tmpfs"
	case 0x794c7630:
		name = "overlayfs"
	case 0xef53:
		name = "ext2/3/4"
	case 0x58465342:
		name = "xfs"
	case 0x9123683e:
		name = "btrfs"
	}
	return map[string]any{"name": name, "type_magic": fmt.Sprintf("0x%x", s.Type), "block_size": s.Bsize, "physical_device": "not identified"}
}
