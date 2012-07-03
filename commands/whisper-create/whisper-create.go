package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"flag"
	"log"
)

func usage() {
	log.Fatal("Wrong number of arguments")
}

func main() {
	flag.Parse()
	if flag.NArg() < 2 {
		usage()
	}
	args := flag.Args()
	path := args[0]
	archiveStrings := args[1:]

	var archives whisper.ArchiveInfos
	for _, s := range archiveStrings {
		archive, err := whisper.ParseArchiveInfo(s)
		if err != nil {
			log.Fatal(err)
		}
		archives = append(archives, archive)
	}

	whisper.Create(path, archives, 0.5, whisper.AGGREGATION_AVERAGE, false)
}
