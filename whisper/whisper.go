package whisper

import (
	"encoding/binary"
	"io"
	"os"
)

type Metadata struct {
	AggregationMethod uint32
	MaxRetention      uint32
	XFilesFactor      float32
	ArchiveCount      uint32
}

type ArchiveInfo struct {
	Offset          uint32
	SecondsPerPoint uint32
	Points          uint32
}

type Header struct {
	Metadata Metadata
	Archives []ArchiveInfo
}

type Archive []Point

type Point struct {
	Timestamp uint32
	Value     float64
}

func ReadHeader(buf io.ReadSeeker) (header Header, err error) {
	currentPos, err := buf.Seek(0, 1)
	if err != nil {
		return
	}
	defer func() {
		// Try to return to the original position when we exit
		_, e := buf.Seek(currentPos, 0)
		if e != nil {
			err = e
		}
		return
	}()

	// Start at the beginning of the file
	_, err = buf.Seek(0, 0)
	if err != nil {
		return
	}

	// Read metadata
	var metadata Metadata
	err = binary.Read(buf, binary.BigEndian, metadata)
	if err != nil {
		return
	}
	header.Metadata = metadata

	// Read archive info
	archives := make([]ArchiveInfo, metadata.ArchiveCount)
	for i := uint32(0); i < metadata.ArchiveCount; i++ {
		err = binary.Read(buf, binary.BigEndian, archives[i])
		if err != nil {
			return
		}
	}
	header.Archives = archives

	return
}

func Create(path string, archives []ArchiveInfo, xFilesFactor float32, aggregationMethod uint32, sparse bool) (err error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)

	oldest := uint32(0)
	for _, archive := range archives {
		age := archive.SecondsPerPoint * archive.Points
		if age > oldest {
			oldest = age
		}
	}

	metadata := Metadata{
		AggregationMethod: aggregationMethod,
		XFilesFactor:      xFilesFactor,
		ArchiveCount:      uint32(len(archives)),
		MaxRetention:      oldest,
	}
	err = binary.Write(file, binary.BigEndian, metadata)
	if err != nil {
		return
	}

	headerSize := uint32(binary.Size(metadata) + (binary.Size(archives[0]) * len(archives)))
	pointSize := uint32(binary.Size(Point{}))
	archiveOffsetPointer := headerSize

	for _, archive := range archives {
		archive.Offset = archiveOffsetPointer
		err = binary.Write(file, binary.BigEndian, archive)
		if err != nil {
			return
		}
		archiveOffsetPointer += archive.Points * pointSize
	}

	if sparse {
		file.Seek(int64(archiveOffsetPointer-headerSize-1), 0)
		file.Write([]byte{0})
	} else {
		remaining := archiveOffsetPointer - headerSize
		chunkSize := uint32(16384)
		buf := make([]byte, chunkSize)
		for remaining > chunkSize {
			file.Write(buf)
			remaining -= chunkSize
		}
		file.Write(buf[:remaining])
	}

	return
}
