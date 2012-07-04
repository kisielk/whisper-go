package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"flag"
	"fmt"
	"log"
)

func usage() {
	log.Fatal("Wrong number of arguments")
}

func main() {
	flag.Parse()	
	if flag.NArg() != 1 {
		usage()
	}

	path := flag.Args()[0]

	w, err := whisper.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("%s:\n", path)
	fmt.Printf("Maximum retention:\t%d\n", w.Header.Metadata.MaxRetention)
	fmt.Printf("X-Files factor:\t\t%f\n", w.Header.Metadata.XFilesFactor)
	fmt.Printf("Number of archives:\t%d\n", w.Header.Metadata.ArchiveCount)
	fmt.Printf("\n")

	for i, archive := range w.Header.Archives {
		fmt.Printf("Archive %d:\n", i)
		fmt.Printf("Seconds per point:\t%d\n", archive.SecondsPerPoint)
		fmt.Printf("Points:\t\t\t%d\n", archive.Points)
		fmt.Printf("Retention:\t\t%d\n", archive.SecondsPerPoint * archive.Points)
	}
}
