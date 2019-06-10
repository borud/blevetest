package main

import (
	"log"
	"os"

	"github.com/borud/blevetest/pkg/index"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <tar.gz> <index>", os.Args[0])
	}

	tgzFilename := os.Args[1]
	indexFilename := os.Args[2]

	idx, err := index.Create(indexFilename)
	if err != nil {
		log.Fatalf("Error indexing '%s' into '%s': %v", tgzFilename, indexFilename, err)
	}
	defer idx.Close()

	idx.IndexFromTarGz(tgzFilename, 100)
}
