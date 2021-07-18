package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/storage"
)

func main() {
	if len(os.Args) != 4 {
		log.Fatalf("Usage: %s AK SK BUCKET", os.Args[0])
	}
	ak := os.Args[1]
	sk := os.Args[2]
	bucket := os.Args[3]

	credentials := auth.New(ak, sk)
	bucketManager := storage.NewBucketManager(credentials, &storage.Config{UseHTTPS: true})

	var (
		marker    = ""
		filesChan = make(chan []storage.ListItem, 1024)
		wg        sync.WaitGroup
	)

	cleanWorkers(bucket, bucketManager, filesChan, &wg)

	for {
		files := make([]storage.ListItem, 0, 100)
		listedFilesChan, err := bucketManager.ListBucket(bucket, "", "", marker)
		if err != nil {
			log.Fatalf("ListBucket() Error: %s", err)
		}
		for listedFile := range listedFilesChan {
			marker = listedFile.Marker
			if len(files) < cap(files) {
				files = append(files, listedFile.Item)
			} else {
				filesChan <- files
				files = make([]storage.ListItem, 0, 100)
			}
		}
		if len(files) > 0 {
			filesChan <- files
		}
		if marker == "" {
			break
		}
	}

	close(filesChan)
	wg.Wait()
}

func cleanWorkers(bucket string, bucketManager *storage.BucketManager, filesChan <-chan []storage.ListItem, wg *sync.WaitGroup) {
	for i := 0; i < 20; i++ {
		wg.Add(1)
		cleanWorker(bucket, bucketManager, filesChan, wg)
	}
}

func cleanWorker(bucket string, bucketManager *storage.BucketManager, filesChan <-chan []storage.ListItem, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for files := range filesChan {
			ops := make([]string, len(files))
			for i, file := range files {
				ops[i] = storage.URIDelete(bucket, file.Key)
			}
			rets, err := bucketManager.Batch(ops)
			if err != nil {
				log.Printf("BatchDelete() Error: %s", err)
			}
			for i, ret := range rets {
				if ret.Code != http.StatusOK {
					log.Printf("BatchDelete() Partial Error for %s: %s", files[i].Key, err)
				}
			}
			log.Printf("Delete %d files", len(files))
		}
	}()
}
