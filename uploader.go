package main

import (
	"context"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/fsnotify/fsnotify"
	"go.ntppool.org/common/logger"
	"golang.org/x/exp/slices"
)

func uploader(ctx context.Context, client *http.Client, logpath, apiURL string) error {

	log := logger.Setup()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// todo: detect if the list is full and rescan directory
	processCh := make(chan string, 10000)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// log.Info("event info", "event", event)
				// if event.Has(fsnotify.Write) {
				// 	log.Info("modified file", "name", event.Name)
				// }

				if event.Has(fsnotify.Rename) || event.Has(fsnotify.Create) {
					if isAvroFileName(event.Name) {
						processCh <- event.Name
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Error("fswatcher error", "err", err)
			}
		}
	}()

	err = watcher.Add(logpath)
	if err != nil {
		return err
	}

	dir, err := os.ReadDir(logpath)
	if err != nil {
		return err
	}

	go func() {

		for _, fi := range dir {
			files := []string{}
			if isAvroFileName(fi.Name()) {
				files = append(files, fi.Name())
			}
			slices.Sort(files)
			for _, n := range files {
				processCh <- path.Join(logpath, n)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case p := <-processCh:
			log.Debug("change to file", "path", p)
			err := upload(ctx, client, p, apiURL)
			if err != nil {
				log.Error("error uploading", "path", p, "err", err)
				continue
			}
			err = os.Remove(p)
			if err != nil {
				log.Error("error removing log", "path", p, "err", err)
			}
		}
	}
}

func isAvroFileName(name string) bool {
	return strings.HasSuffix(name, ".avro")
}

func upload(ctx context.Context, client *http.Client, filePath, apiURL string) error {

	log := logger.Setup()

	_, baseName := path.Split(filePath)
	log = log.With("file", baseName)

	fh, err := os.Open(filePath)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, fh)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Info("uploaded", "code", resp.StatusCode)
	} else {
		log.Warn("upload error", "code", resp.StatusCode, "status", resp.Status)
	}

	return nil
}
