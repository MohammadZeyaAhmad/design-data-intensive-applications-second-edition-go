package main

import (
	"encoding/json"
	"flag"
	"os"

	cachelab "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch02/cache"
)

func runCache(args []string) error {
	flags := flag.NewFlagSet("lab cache", flag.ContinueOnError)
	output := flags.String("output", "", "JSON counterexample report")
	if err := flags.Parse(args); err != nil {
		return err
	}
	report, err := cachelab.Run()
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if *output != "" {
		if err = saveReport(*output, b); err != nil {
			return err
		}
	}
	_, err = os.Stdout.Write(b)
	return err
}
