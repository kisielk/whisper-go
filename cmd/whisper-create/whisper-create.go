package main

import (
	"flag"
	"fmt"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"os"
	"strings"
)

type aggregationFlag whisper.AggregationMethod

func (f *aggregationFlag) String() string {
	return f.String()
}

func (f *aggregationFlag) Set(s string) error {
	var m whisper.AggregationMethod
	s = strings.ToLower(s)
	switch s {
	case "average":
		m = whisper.AggregationAverage
	case "last":
		m = whisper.AggregationLast
	case "sum":
		m = whisper.AggregationSum
	case "max":
		m = whisper.AggregationMax
	case "min":
		m = whisper.AggregationMin
	default:
		m = whisper.AggregationUnknown
	}
	*f = aggregationFlag(m)
	return nil
}

var aggregationMethod = aggregationFlag(whisper.AggregationAverage)
var xFilesFactor float64

func main() {
	flag.Var(&aggregationMethod, "aggregationMethod", "aggregation method to use")
	flag.Float64Var(&xFilesFactor, "xFilesFactor", whisper.DefaultXFilesFactor, "x-files factor")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [OPTION]... [FILE] [PRECISION:RETENTION]...\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	method := whisper.AggregationMethod(aggregationMethod)

	log.SetFlags(0)

	if flag.NArg() < 1 {
		flag.Usage()
		log.Fatal("error: you must specify a filename")
	}

	if flag.NArg() < 2 {
		flag.Usage()
		log.Fatal("error: you must specify at least one PRECISION:RETENTION pair for the archive\n")
	}

	if method == whisper.AggregationUnknown {
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

	_, err := whisper.Create(path, archives, whisper.CreateOptions{XFilesFactor: float32(xFilesFactor), AggregationMethod: method})
	if err != nil {
		log.Fatal(err)
	}
}
