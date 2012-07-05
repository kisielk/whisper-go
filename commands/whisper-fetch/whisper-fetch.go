package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"flag"
	"fmt"
	"log"
	"time"
)

var from, until uint

func usage() {
	log.Fatal("Wrong number of arguments")
}

func main() {
	now := uint(time.Now().Unix())
	yesterday := uint(time.Now().Add(-24 * time.Hour).Unix())
	flag.UintVar(&from, "from", yesterday, "Unix epoch time of the beginning of the requested interval. (default: 24 hours ago)")
	flag.UintVar(&until, "until", now, "Unix epoch time of the end of the requested interval. (default: now)")
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
	}

	path := flag.Args()[0]
	fromTime := uint32(from)
	untilTime := uint32(until)

	w, err := whisper.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	interval, points, err := w.FetchUntil(fromTime, untilTime)

	fmt.Printf("Values in interval %q", interval)
	for i, p := range points {
		fmt.Printf("%d %q\n", i, p)
	}
	return
}
