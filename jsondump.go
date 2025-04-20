package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/abh/geodns/v3/querylog"
	"github.com/hamba/avro/v2/ocf"
	"go.ntppool.org/common/logger"
)

func dumpJSON(files []string) {
	log := logger.Setup()

	for _, fileName := range files {
		err := readFile(log, fileName)
		if err != nil {
			log.Error("could not read file", "err", err)
		}
	}
}

func readFile(log *slog.Logger, fileName string) error {
	log = log.With("file", fileName)
	fh, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fh.Close()
	dec, err := ocf.NewDecoder(fh)
	if err != nil {
		return fmt.Errorf("could not setup decoder: %w", err)
	}

	for dec.HasNext() {
		e := querylog.Entry{}
		err := dec.Decode(&e)
		if err != nil {
			log.Warn("could not decode entry", "err", err)
			continue
		}
		js, err := json.Marshal(e)
		if err != nil {
			log.Warn("could not marshal json", "err", err)
			continue
		}
		fmt.Printf("%s\n", js)
	}
	return nil
}
