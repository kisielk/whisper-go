package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"flag"
	"fmt"
	"log"
	"os"
)

var aggregationMethod whisper.AggregationMethod = whisper.AGGREGATION_AVERAGE
var xFilesFactor float64

func main() {
	flag.Var(&aggregationMethod, "aggregationMethod", "aggregation method to use")
	flag.Float64Var(&xFilesFactor, "xFilesFactor", 0.5, "x-files factor")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [OPTION]... [FILE] [PRECISION:RETENTION]...\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	log.SetFlags(0)

	if flag.NArg() < 1 {
		flag.Usage()
		log.Fatal("error: you must specify a filename")
	}

	if flag.NArg() < 2 {
		flag.Usage()
		log.Fatal("error: you must specify at least one PRECISION:RETENTION pair for the archive\n")
	}

	if aggregationMethod == whisper.AGGREGATION_UNKNOWN {
		flag.Usage()
		log.Fatal(fmt.Sprintf("error: unknown aggregation method \"%v\"", aggregationMethod.String()))
	}

	args := flag.Args()
	path := args[0]
	archiveStrings := args[1:]

	var archives []whisper.ArchiveInfo
	for _, s := range archiveStrings {
		archive, err := whisper.ParseArchiveInfo(s)
		if err != nil {
			log.Fatal(fmt.Sprintf("error: %s", err))
		}
		archives = append(archives, archive)
	}

	err := whisper.Create(path, archives, float32(xFilesFactor), aggregationMethod, false)
	if err != nil {
		log.Fatal(err)
	}
}
