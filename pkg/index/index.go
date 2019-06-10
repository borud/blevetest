package index

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/char/html"
)

type Index struct {
	indexFilename string
	index         bleve.Index
}

type IndexEntry struct {
	ID       string `json:"id"`
	Contents string `json:"contents"`
}

func Open(indexFilename string) (*Index, error) {
	index, err := bleve.Open(indexFilename)
	if err != nil {
		return nil, err
	}

	return &Index{
		indexFilename: indexFilename,
		index:         index,
	}, nil
}

func Create(indexFilename string) (*Index, error) {
	index, err := bleve.New(indexFilename, bleve.NewIndexMapping())
	if err != nil {
		return nil, err
	}

	return &Index{
		indexFilename: indexFilename,
		index:         index,
	}, nil
}

func (idx *Index) Close() error {
	return idx.index.Close()
}

func (idx *Index) IndexFromTarGz(tgzFilename string, batchSize int) error {
	// Open the source file
	f, err := os.Open(tgzFilename)
	if err != nil {
		return fmt.Errorf("Unable to open %s: %v", tgzFilename, err)
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("Unable to unzip %s: %v", tgzFilename, err)
	}
	tarReader := tar.NewReader(gzr)
	if err != nil {
		return fmt.Errorf("Unable to initialize bleve index %s: %v", idx.indexFilename, err)
	}

	batch := idx.index.NewBatch()
	batchCount := batchSize - 1

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("Unable to read tar file %s: %v", tgzFilename, err)
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		data, err := ioutil.ReadAll(tarReader)
		if err != nil {
			return fmt.Errorf("Error reading %s from %s: %v", header.Name, tgzFilename, err)
		}

		ent := &IndexEntry{ID: header.Name, Contents: string(data)}

		err = batch.Index(header.Name, ent)
		if err != nil {
			return fmt.Errorf("Error indexing %s from %s: %v", header.Name, tgzFilename, err)
		}

		batchCount++
		if batchCount == batchSize {
			batchCount = 0

			log.Printf("Indexing batch...")
			start := time.Now()
			err = idx.index.Batch(batch)
			elapsed := time.Since(start)

			log.Printf(" batch size=%d time=%s ops/sec=%f", batchSize, elapsed, float64(batchSize)/elapsed.Seconds())
			if err != nil {
				return fmt.Errorf("Error indexing %s from %s: %v", header.Name, tgzFilename, err)
			}
			batch = idx.index.NewBatch()
		}
	}

	if batch.Size() > 0 {
		err = idx.index.Batch(batch)
		if err != nil {
			return fmt.Errorf("Error indexing last batch from %s: %v", tgzFilename, err)
		}
	}

	return nil
}

var re = regexp.MustCompile("\\n")

func (idx *Index) Search(terms string) error {
	query := bleve.NewQueryStringQuery(terms)
	search := bleve.NewSearchRequest(query)

	highlightRequest := bleve.NewHighlightWithStyle(html.Name)
	highlightRequest.AddField("contents")
	search.Highlight = highlightRequest

	search.Size = 20

	searchResults, err := idx.index.Search(search)
	if err != nil {
		return err
	}

	fmt.Print("\n\n")
	for n, hit := range searchResults.Hits {
		fmt.Printf("%4d | id = '%s' score = '%f'\n", n, hit.ID, hit.Score)
		fmt.Print("--------------------------------------------------\n")

		for fld, val := range hit.Fragments {
			fmt.Printf("  Fragment field='%s' val='%s'\n", fld, re.ReplaceAllString(strings.Join(val, " "), " "))
		}

		for k, loc := range hit.Locations {
			fmt.Printf("  Location - %s\n", k)
			for term, positions := range loc {
				fmt.Printf("    - Term: %s\n", term)
				for _, pos := range positions {
					fmt.Printf("      - Positions: pos=%d start=%d end=%d\n", pos.Pos, pos.Start, pos.End)
				}
			}
		}
		fmt.Print("\n")
	}

	return nil
}
