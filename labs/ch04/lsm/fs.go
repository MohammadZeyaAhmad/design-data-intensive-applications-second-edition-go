package lsm

import (
	"io"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskio"
)

type dataFile = diskio.File
type filesystem = diskio.FS
type osFS = diskio.OS

func writeAll(w io.Writer, b []byte) (int, error) { return diskio.WriteAll(w, b) }
