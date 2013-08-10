package main

import (
	"flag"
	"fmt"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"os"
)

func main() {
	flag.Parse()
	filename := flag.Arg(0)
	if filename == "" {
		flag.Usage()
		os.Exit(1)
	}

	db, err := whisper.Open(filename)
	if err != nil {
		fmt.Println("could not open database:", err)
		os.Exit(1)
	}
	defer db.Close()
	dumpHeader(db)
	dumpArchiveHeaders(db)
	dumpArchives(db)
}

func dumpHeader(w *whisper.Whisper) {
	fmt.Println("Meta data:")
	fmt.Println("  aggregation method:", w.Header.Metadata.AggregationMethod)
	fmt.Println("  max retention:", w.Header.Metadata.MaxRetention)
	fmt.Println("  xFilesFactor:", w.Header.Metadata.XFilesFactor)
	fmt.Println()
}

func dumpArchiveHeaders(w *whisper.Whisper) {
	for i, archive := range w.Header.Archives {
		fmt.Println("Archive", i, "info:")
		fmt.Println("  offset:", archive.Offset)
		fmt.Println("  seconds per point", archive.SecondsPerPoint)
		fmt.Println("  points", archive.Points)
		fmt.Println("  retention", archive.Retention())
		fmt.Println("  size", archive.Size())
		fmt.Println()
	}
}

func dumpArchives(w *whisper.Whisper) {
	for i := range w.Header.Archives {
		fmt.Println("Archive", i, "data:")
		points, err := w.DumpArchive(i)
		if err != nil {
			log.Fatalln("failed to read archive:", err)
		}
		for n, point := range points {
			fmt.Printf("%d: %d, %10.35g\n", n, point.Timestamp, point.Value)
		}
	}
}
