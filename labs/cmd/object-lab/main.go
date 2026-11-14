package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	objectstorage "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch06/object-storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		f := flag.NewFlagSet("object-lab worker", flag.ContinueOnError)
		job := f.String("job", "", "owned worker job path")
		if err := f.Parse(os.Args[2:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *job == "" {
			return errors.New("worker requires --job")
		}
		return objectstorage.RunWorker(ctx, *job)
	}
	f := flag.NewFlagSet("object-lab", flag.ContinueOnError)
	profile := f.String("profile", "file", "file, emulator, or explicit cloud S3 integration")
	python := f.String("python", os.Getenv("ATLAS_OBJECT_MOTO_PYTHON"), "pinned emulator Python executable")
	script := f.String("moto-script", "", "S3 emulator launcher path")
	output := f.String("output", "", "JSON report path")
	workdir := f.String("workdir", "", "temporary cluster/job parent")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected object-storage argument")
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	cloud := objectstorage.S3Config{Endpoint: os.Getenv("ATLAS_OBJECT_S3_ENDPOINT"), Region: os.Getenv("ATLAS_OBJECT_S3_REGION"), Bucket: os.Getenv("ATLAS_OBJECT_S3_BUCKET"), AccessKey: os.Getenv("ATLAS_OBJECT_S3_ACCESS_KEY"), SecretKey: os.Getenv("ATLAS_OBJECT_S3_SECRET_KEY"), SessionToken: os.Getenv("ATLAS_OBJECT_S3_SESSION_TOKEN")}
	if *profile == "cloud" && (cloud.Region == "" || cloud.Bucket == "" || cloud.AccessKey == "" || cloud.SecretKey == "") {
		return errors.New("cloud profile requires ATLAS_OBJECT_S3_REGION, ATLAS_OBJECT_S3_BUCKET, ATLAS_OBJECT_S3_ACCESS_KEY and ATLAS_OBJECT_S3_SECRET_KEY")
	}
	r, err := objectstorage.Run(ctx, objectstorage.RunOptions{Profile: *profile, Binary: binary, Python: *python, ServerScript: *script, Workdir: *workdir, Cloud: cloud})
	if err != nil {
		return err
	}
	if *output != "" {
		if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
			return err
		}
		if err = objectstorage.SaveJSON(*output, r); err != nil {
			return err
		}
	}
	fmt.Printf("object storage %s: contract %d/%d CAS winners, generation %d, %d rows, %d process-kill boundaries\n", r.Profile, r.Contract.CreatorWins, r.Contract.ReplacementWins, r.Native.FinalGeneration, r.Native.FinalRows, len(r.Native.Crashes))
	return nil
}
