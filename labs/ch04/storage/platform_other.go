//go:build !linux

package storage

func filesystemInfo(string) map[string]any {
	return map[string]any{"type": "unreported on this platform"}
}
