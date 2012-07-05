package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"flag"
	"log"
	"time"
	"strings"
	"strconv"
)

func usage() {
	log.Fatal("Wrong number of arguments")
}

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		usage()
	}

	now := uint32(time.Now().Unix())

	// Open the database
	args := flag.Args()
	path := args[0]
	w, err := whisper.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	// Parse all the points
	var points = make([]whisper.Point, len(args) - 1)
	for _, p := range args[1:] {
		splitP := strings.Split(p, ":")

		if len(splitP) != 2 {
			log.Fatalf("invalid point %s: %s", p, err)
		}


		// Parse the timestamp
		var timestamp uint32
		timestampString := splitP[0]
		if timestampString == "N" {
			timestamp = now
		} else {
			timestamp64, err := strconv.ParseUint(timestampString, 10, 32)
			if err != nil {
				log.Fatalf("invalid timestamp %s: %s", timestampString, err)
			}
			timestamp = uint32(timestamp64)
		}

		// Parse the value
		value, err := strconv.ParseFloat(splitP[1], 64)
		if err != nil {
			log.Fatalf("invalid value: %s", splitP[1])
		}

		points = append(points, whisper.Point{timestamp, value})
	}

	err = w.UpdateMany(points)
	if err != nil {
		log.Fatal("failed to update database: %s", err)
	}

}
