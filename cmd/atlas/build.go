package main

import "runtime/debug"

func buildRevision() string {
	revision, dirty := "development", false
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
	}
	if dirty {
		return revision + "-dirty"
	}
	return revision
}
