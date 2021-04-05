package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/luoos/lite-gcsfuse-demo/gcsfs"

	"github.com/jacobsa/fuse"
	"cloud.google.com/go/storage"
)

func getGCSDemoObject(bucketName, objectName string) *storage.ObjectHandle {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	ob := client.Bucket(bucketName).Object(objectName)
	return ob
}

func main() {
	bucketName := flag.String("bucket", "", "GCS bucket name, required")
	objectName := flag.String("object", "", "GCS object name, required")
	mountPoint := flag.String("mount", "/tmp/y", "Mount point, optional")
	flag.Parse()
	if *bucketName == "" || *objectName == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if _, err := os.Stat(*mountPoint); os.IsNotExist(err) {
		fmt.Printf("Mount point doesn't exist, creating %s\n", *mountPoint)
		os.Mkdir(*mountPoint, 0777)
	}

	obj := getGCSDemoObject(*bucketName, *objectName)
	server, err := gcsfs.NewGCSFileSystem(obj)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}

	cfg := &fuse.MountConfig{
		ReadOnly: false,
	}

	mfs, err := fuse.Mount(*mountPoint, server, cfg)
	if err != nil {
		flag.PrintDefaults()
		log.Fatalf("Mount: %v", err)
	}
	fmt.Printf("The FUSE filesystem is mounted on %s\n", *mountPoint)
	fmt.Printf("call `fusermount -u %s` to stop\n", *mountPoint)

	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}
}