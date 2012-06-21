package whisper

import (
	"encoding/binary"
	"io"
	"os"
	"time"
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

func (a ArchiveInfo) Retention() uint32 {
	return a.SecondsPerPoint * a.Points
}

func (a ArchiveInfo) Size() uint32 {
	return a.Points * pointSize
}

func (a ArchiveInfo) End() uint32 {
	return a.Offset + a.Size()
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

type Whisper struct {
	Header Header
	file   *os.File
}

var pointSize, metadataSize, archiveSize uint32

func init() {
	pointSize = uint32(binary.Size(Point{}))
	metadataSize = uint32(binary.Size(Metadata{}))
	archiveSize = uint32(binary.Size(Archive{}))
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

	headerSize := metadataSize + (archiveSize * uint32(len(archives)))
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

func Open(path string) (whisper Whisper, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return
	}

	header, err := ReadHeader(file)
	if err != nil {
		return
	}
	whisper = Whisper{Header: header, file: file}
	return
}

func (w Whisper) Update(point Point) (err error) {
	now := uint32(time.Now().Unix())
	diff := now - point.Timestamp
	if !((diff < w.Header.Metadata.MaxRetention) && diff >= 0) {
		// TODO: Return an error
		return
	}

	var lowerArchives []ArchiveInfo
	var currentArchive ArchiveInfo
	for i, currentArchive := range w.Header.Archives {
		if currentArchive.Retention() < diff {
			continue
		}
		lowerArchives = w.Header.Archives[i+1:]
	}

	point.Timestamp = point.Timestamp - (point.Timestamp % currentArchive.SecondsPerPoint)
	_, err = w.file.Seek(int64(currentArchive.Offset), 0)
	if err != nil {
		return
	}

	var basePoint Point
	err = binary.Read(w.file, binary.BigEndian, basePoint)
	if err != nil {
		return
	}

	if basePoint.Timestamp == 0 {
		basePoint, err = w.readPoint(currentArchive.Offset)
		if err != nil {
			return
		}
	} else {
		myOffset := pointOffset(currentArchive, point.Timestamp, basePoint.Timestamp)
		err = w.writePoint(myOffset, point)
		if err != nil {
			return
		}
	}

	higherArchive := currentArchive
	var lowerArchive ArchiveInfo
	for _, lowerArchive = range(lowerArchives) {
		result, e := w.propagate(point.Timestamp, higherArchive, lowerArchive)
		if ! result {
			break
		}
		if e != nil {
			err = e
			return
		}
		higherArchive = lowerArchive
	}

	return
}

func pointOffset(archive ArchiveInfo, timestamp uint32, baseTimestamp uint32) uint32 {
	timeDistance := timestamp - baseTimestamp
	pointDistance := timeDistance / archive.SecondsPerPoint
	byteDistance := pointDistance * pointSize
	return archive.Offset + (byteDistance % archive.Size())
}

func (w Whisper) propagate(timestamp uint32, higher ArchiveInfo, lower ArchiveInfo) (result bool, err error) {
	lowerIntervalStart := timestamp - (timestamp % lower.SecondsPerPoint)

	basePoint, err := w.readPoint(higher.Offset)
	if err != nil {
		return
	}

	var higherFirstOffset uint32
	if basePoint.Timestamp == 0 {
		higherFirstOffset = higher.Offset
	} else {
		higherFirstOffset = pointOffset(higher, lowerIntervalStart, basePoint.Timestamp)
	}

	numHigherPoints := lower.SecondsPerPoint - higher.SecondsPerPoint
	higherSize := numHigherPoints * pointSize
	relativeFirstOffset := higherFirstOffset - higher.Offset
	relativeLastOffset := (relativeFirstOffset + higherSize) % higher.Size()
	higherLastOffset := relativeLastOffset + higher.Offset

	var higherPoints []Point
	if higherFirstOffset < higherLastOffset {
		// The selection is in the middle of the archive. eg: --####---
		higherPoints = make([]Point, (higherLastOffset - higherFirstOffset) / pointSize)
		err = w.readPoints(higherFirstOffset, higherPoints)
		if err != nil {
			return
		}
	} else {
		// The selection wraps over the end of the archive. eg: ##----###
		numEndPoints := (higher.End() - higherFirstOffset) / pointSize
		numBeginPoints := (higherLastOffset - higher.Offset) / pointSize
		higherPoints = make([]Point, numBeginPoints + numEndPoints)

		err = w.readPoints(higherFirstOffset, higherPoints[:numEndPoints])
		if err != nil {
			return
		}
		err = w.readPoints(higher.Offset, higherPoints[numEndPoints:])
		if err != nil {
			return
		}
	}


	return
}

func (w Whisper) readPoint(offset uint32) (point Point, err error){
	w.file.Seek(int64(offset), 0)
	err = binary.Read(w.file, binary.BigEndian, point)
	return
}

func (w Whisper) readPoints(offset uint32, points []Point) (err error) {
	w.file.Seek(int64(offset), 0)
	err = binary.Read(w.file, binary.BigEndian, points)
	return
}

func (w Whisper) writePoint(offset uint32, point Point) (err error) {
	w.file.Seek(int64(offset), 0)
	err = binary.Write(w.file, binary.BigEndian, point)
	return
}
