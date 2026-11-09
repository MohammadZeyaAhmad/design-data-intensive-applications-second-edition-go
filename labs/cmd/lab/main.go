package main

import (
	"encoding/json"
	"fmt"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch08/lostupdate"
	"os"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "lost-update" {
		fmt.Fprintln(os.Stderr, "usage: lab lost-update")
		os.Exit(1)
	}
	report := lostupdate.Run()
	if !report.ExpectedCounterexample || report.UnsafeFinal != 1 || report.AtomicFinal != 2 {
		fmt.Fprintln(os.Stderr, "unexpected model result")
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
