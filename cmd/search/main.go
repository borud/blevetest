package main

import (
	"log"
	"os"
	"strings"

	"github.com/borud/blevetest/pkg/index"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <index> <search terms>", os.Args[0])
	}

	indexFilename := os.Args[1]
	searchTerms := strings.Join(os.Args[2:], " ")

	idx, err := index.Open(indexFilename)
	if err != nil {
		log.Fatalf("Error opening index '%s': %v", indexFilename, err)
	}

	idx.Search(searchTerms)
}
