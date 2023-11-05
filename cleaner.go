package main

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"go.ntppool.org/common/logger"
)

// directoryCleaner runs until ctx is cancelled and occasionally
// cleans stray tmp files from the directory by deleting or
// renaming them.
func directoryCleaner(ctx context.Context, logpath string) error {
	log := logger.FromContext(ctx)

	for {
		log.Info("running directory cleaner")
		dir, err := os.ReadDir(logpath)
		if err != nil {
			return err
		}

		for _, f := range dir {
			log := log.With("name", f.Name())
			if f.IsDir() {
				continue
			}
			info, err := f.Info()
			if err != nil {
				log.Info("could not info", "err", err)
				continue
			}

			if !strings.HasSuffix(f.Name(), ".tmp") {
				continue
			}

			if info.ModTime().After(time.Now().Add(-90 * time.Minute)) {
				// too new, check later
				continue
			}
			// log.Info("cleanup", "f", f.Name, "info", info)
			oldName := path.Join(logpath, f.Name())

			if info.Size() == 0 {
				log.Info("deleting empty file")
				os.Remove(oldName)
				continue
			}

			newName := oldName[0:strings.Index(oldName, ".tmp")]

			err = os.Rename(oldName, newName)
			if err != nil {
				log.Warn("could not rename", "target", newName, "err", err)
				continue
			}
		}

		select {
		case <-time.After(2 * time.Hour):
		case <-ctx.Done():
			return nil
		}
	}

}
