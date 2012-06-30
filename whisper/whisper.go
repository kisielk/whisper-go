package whisper

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"time"
)

// General metadata about a whisper database
type Metadata struct {
	AggregationMethod uint32  // Aggregation method used. See the AGGREGATION_* constants
	MaxRetention      uint32  // The maximum retention period
	XFilesFactor      float32 // The minimum percentage of known values required to aggregate
	ArchiveCount      uint32  // The number of archives in the database
}

// Metadata about an archive within the database
type ArchiveInfo struct {
	Offset          uint32 // The offset of the archive within the database
	SecondsPerPoint uint32 // The number of seconds of elapsed time represented by a data point
	Points          uint32 // The number of data points
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

type ArchiveInfos []ArchiveInfo

func (a ArchiveInfos) Len() int           { return len(a) }
func (a ArchiveInfos) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ArchiveInfos) Less(i, j int) bool { return a[i].SecondsPerPoint < a[j].SecondsPerPoint }

// The whisper database header, contains metadata
type Header struct {
	Metadata Metadata
	Archives ArchiveInfos
}

type Archive []Point

func (a Archive) Len() int           { return len(a) }
func (a Archive) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Archive) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

type ReverseArchive struct {
	Archive
}

func (r ReverseArchive) Less(i, j int) bool { return r.Archive.Less(j, i) }

type Point struct {
	Timestamp uint32  // Timestamp in seconds past the epoch
	Value     float64 // Data point value
}

type Whisper struct {
	Header Header
	file   *os.File
}

var pointSize, metadataSize, archiveSize uint32

const (
	AGGREGATION_AVERAGE = 1 // Aggregation type using averaging
	AGGREGATION_SUM     = 2 // Aggregation type using sum
	AGGREGATION_LAST    = 3 // Aggregation type using the last value
	AGGREGATION_MAX     = 4 // Aggregation type using the maximum value
	AGGREGATION_MIN     = 5 // Aggregation type using the minimum value
)

func init() {
	pointSize = uint32(binary.Size(Point{}))
	metadataSize = uint32(binary.Size(Metadata{}))
	archiveSize = uint32(binary.Size(Archive{}))
}

// Read the header of a whisper database
func readHeader(buf io.ReadSeeker) (header Header, err error) {
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
	archives := make(ArchiveInfos, metadata.ArchiveCount)
	for i := uint32(0); i < metadata.ArchiveCount; i++ {
		err = binary.Read(buf, binary.BigEndian, archives[i])
		if err != nil {
			return
		}
	}
	header.Archives = archives

	return
}

/* 

Validates a list of ArchiveInfos

The list must:

1. Have at least one ArchiveInfo

2. No archive may be a duplicate of another.

3. Higher precision archives' precision must evenly divide all lower precision archives' precision.

4. Lower precision archives must cover larger time intervals than higher precision archives.

5. Each archive must have at least enough points to consolidate to the next archive

*/
func ValidateArchiveList(archives ArchiveInfos) error {
	//TODO: Better error messages for this function

	sort.Sort(archives)

	// 1.
	if len(archives) == 0 {
		return errors.New("archive list cannot have 0 length")
	}

	for i, archive := range archives {
		if i == (len(archives) - 1) {
			break
		}

		// 2.
		nextArchive := archives[i+1]
		if !(archive.SecondsPerPoint < nextArchive.SecondsPerPoint) {
			return errors.New("No archive may be a duplicate of another")
		}

		// 3.
		if nextArchive.SecondsPerPoint%archive.SecondsPerPoint != 0 {
			return errors.New("Higher precision archives must evenly divide in to lower precision")
		}

		// 4.
		nextRetention := nextArchive.Retention()
		retention := archive.Retention()
		if !(nextRetention > retention) {
			return errors.New("Lower precision archives must cover a larger time interval than higher precision")
		}

		// 5.
		if !(archive.Points >= (nextArchive.SecondsPerPoint / archive.SecondsPerPoint)) {
			return errors.New("Each archive must be able to consolidate the next")
		}

	}
	return nil

}

// Create a new whisper database at a given file path
func Create(path string, archives ArchiveInfos, xFilesFactor float32, aggregationMethod uint32, sparse bool) (err error) {
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

// Open a whisper database
func Open(path string) (whisper Whisper, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return
	}

	header, err := readHeader(file)
	if err != nil {
		return
	}
	whisper = Whisper{Header: header, file: file}
	return
}

// Write a single datapoint to the whisper database
func (w Whisper) Update(point Point) (err error) {
	now := uint32(time.Now().Unix())
	diff := now - point.Timestamp
	if !((diff < w.Header.Metadata.MaxRetention) && diff >= 0) {
		// TODO: Return an error
		return
	}

	// Find the higher-precision archive that covers the timestamp
	var lowerArchives ArchiveInfos
	var currentArchive ArchiveInfo
	for i, currentArchive := range w.Header.Archives {
		if currentArchive.Retention() < diff {
			continue
		}
		lowerArchives = w.Header.Archives[i+1:]
	}

	// Normalize the point's timestamp to the current archive's precision
	point.Timestamp = point.Timestamp - (point.Timestamp % currentArchive.SecondsPerPoint)

	// Write the point
	offset, err := w.pointOffset(currentArchive, point.Timestamp)
	if err != nil {
		return
	}
	err = w.writePoint(offset, point)
	if err != nil {
		return
	}

	// Propagate data down to all the lower resolution archives
	higherArchive := currentArchive
	for _, lowerArchive := range lowerArchives {
		result, e := w.propagate(point.Timestamp, higherArchive, lowerArchive)
		if !result {
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

// Write a series of datapoints to the whisper database
func (w Whisper) UpdateMany(points []Point) (err error) {
	now := uint32(time.Now().Unix())

	archiveIndex := 0
	var currentArchive *ArchiveInfo
	currentArchive = &w.Header.Archives[archiveIndex]
	var currentPoints Archive

PointLoop:
	for _, point := range points {
		age := now - point.Timestamp

		for currentArchive.Retention() < age {
			if len(currentPoints) > 0 {
				sort.Sort(ReverseArchive{currentPoints})
				w.archiveUpdateMany(*currentArchive, currentPoints)
				currentPoints = currentPoints[:0]
			}

			archiveIndex += 1
			if archiveIndex < len(w.Header.Archives) {
				currentArchive = &w.Header.Archives[archiveIndex]
			} else {
				// Drop remaining points that don't fit in the db
				currentArchive = nil
				break PointLoop
			}

		}

		currentPoints = append(currentPoints, point)
	}

	if currentArchive != nil && len(currentPoints) > 0 {
		sort.Sort(ReverseArchive{currentPoints})
		w.archiveUpdateMany(*currentArchive, currentPoints)
	}

	return
}

func quantizeArchive(points Archive, resolution uint32) {
	for i, point := range points {
		points[i].Timestamp = point.Timestamp - (point.Timestamp % resolution)
	}
}

func (w Whisper) archiveUpdateMany(archiveInfo ArchiveInfo, points Archive) (err error) {
	type stampedArchive struct {
		timestamp uint32
		points    Archive
	}
	var archives []stampedArchive
	var currentPoints Archive
	var previousTimestamp, archiveStart uint32

	step := archiveInfo.SecondsPerPoint
	quantizeArchive(points, step)

	for _, point := range points {
		if point.Timestamp == previousTimestamp {
			// ignore values with duplicate timestamps
			continue
		}

		if (previousTimestamp != 0) && (point.Timestamp != previousTimestamp+step) {
			// the current point is not contiguous to the last, start a new series of points

			// append the current archive to the archive list
			archiveStart = previousTimestamp - (uint32(len(currentPoints)) * step)
			archives = append(archives, stampedArchive{archiveStart, currentPoints})

			// start a new archive
			currentPoints = Archive{}
		}

		currentPoints = append(currentPoints, point)
		previousTimestamp = point.Timestamp

	}

	if len(currentPoints) > 0 {
		// If there are any more points remaining after the loop, make a new series for them as well
		archiveStart = previousTimestamp - (uint32(len(currentPoints)) * step)
		archives = append(archives, stampedArchive{archiveStart, currentPoints})
	}

	return
}

func (w Whisper) propagate(timestamp uint32, higher ArchiveInfo, lower ArchiveInfo) (result bool, err error) {
	// The start of the lower resolution archive interval.
	// Essentially a downsampling of the higher resolution timestamp.
	lowerIntervalStart := timestamp - (timestamp % lower.SecondsPerPoint)

	// The offset of the first point in the higher resolution data to be propagated down
	higherFirstOffset, err := w.pointOffset(higher, lowerIntervalStart)
	if err != nil {
		return
	}

	// how many higher resolution points that go in to a lower resolution point
	numHigherPoints := lower.SecondsPerPoint / higher.SecondsPerPoint

	// The total size of the higher resolution points
	higherPointsSize := numHigherPoints * pointSize

	// The realtive offset of the first high res point
	relativeFirstOffset := higherFirstOffset - higher.Offset
	// The relative offset of the last high res point
	relativeLastOffset := (relativeFirstOffset + higherPointsSize) % higher.Size()

	// The actual offset of the last high res point
	higherLastOffset := relativeLastOffset + higher.Offset

	points, err := w.readPointsBetweenOffsets(higher, higherFirstOffset, higherLastOffset)
	if err != nil {
		return
	}

	var neighborPoints []Point
	currentInterval := lowerIntervalStart
	for i := 0; i < len(points); i += 2 {
		if points[i].Timestamp == currentInterval {
			neighborPoints = append(neighborPoints, points[i])
		}
		currentInterval += higher.SecondsPerPoint
	}

	knownPercent := float32(len(neighborPoints))/float32(len(points)) < w.Header.Metadata.XFilesFactor
	if len(neighborPoints) == 0 || knownPercent {
		// There's nothing to propagate
		return false, nil
	}

	aggregatePoint, err := aggregate(w.Header.Metadata.AggregationMethod, neighborPoints)
	if err != nil {
		return
	}
	aggregatePoint.Timestamp = lowerIntervalStart
	aggregateOffset, err := w.pointOffset(lower, aggregatePoint.Timestamp)
	if err != nil {
		return
	}

	err = w.writePoint(aggregateOffset, aggregatePoint)
	if err != nil {
		return
	}

	return true, nil

}

/*

Set the aggregation method for the database

The value of aggregationMethod must be one of the AGGREGATION_* constants

*/
func (w Whisper) setAggregationMethod(aggregationMethod uint32) (err error) {
	//TODO: Validate the value of aggregationMethod

	w.Header.Metadata.AggregationMethod = aggregationMethod
	_, err = w.file.Seek(0, 0)
	if err != nil {
		return
	}

	err = binary.Write(w.file, binary.BigEndian, w.Header.Metadata)
	return
}

// Read a single point from an offset in the database
func (w Whisper) readPoint(offset uint32) (point Point, err error) {
	points := make([]Point, 1)
	err = w.readPoints(offset, points)
	point = points[0]
	return
}

// Read a slice of points from an offset in the database
func (w Whisper) readPoints(offset uint32, points []Point) (err error) {
	_, err = w.file.Seek(int64(offset), 0)
	if err != nil {
		return
	}
	err = binary.Read(w.file, binary.BigEndian, points)
	return
}

func (w Whisper) readPointsBetweenOffsets(archive ArchiveInfo, startOffset, endOffset uint32) (points []Point, err error) {
	archiveStart := archive.Offset
	archiveEnd := archive.End()
	if startOffset < endOffset {
		// The selection is in the middle of the archive. eg: --####---
		points = make([]Point, (endOffset-startOffset)/pointSize)
		err = w.readPoints(startOffset, points)
		if err != nil {
			return
		}
	} else {
		// The selection wraps over the end of the archive. eg: ##----###
		numEndPoints := (archiveEnd - startOffset) / pointSize
		numBeginPoints := (endOffset - archiveStart) / pointSize
		points = make([]Point, numBeginPoints+numEndPoints)

		err = w.readPoints(startOffset, points[:numEndPoints])
		if err != nil {
			return
		}
		err = w.readPoints(archiveStart, points[numEndPoints:])
		if err != nil {
			return
		}
	}
	return
}

// Write a point to an offset in the database
func (w Whisper) writePoint(offset uint32, point Point) (err error) {
	w.file.Seek(int64(offset), 0)
	err = binary.Write(w.file, binary.BigEndian, point)
	return
}

// Get the offset of a timestamp within an archive
func (w Whisper) pointOffset(archive ArchiveInfo, timestamp uint32) (offset uint32, err error) {
	basePoint, err := w.readPoint(0)
	if err != nil {
		return
	}
	if basePoint.Timestamp == 0 {
		// The archive has never been written, this will be the new base point
		offset = archive.Offset
	} else {
		timeDistance := timestamp - basePoint.Timestamp
		pointDistance := timeDistance / archive.SecondsPerPoint
		byteDistance := pointDistance * pointSize
		offset = archive.Offset + (byteDistance % archive.Size())
	}
	return
}

func aggregate(aggregationMethod uint32, points []Point) (point Point, err error) {
	switch aggregationMethod {
	case AGGREGATION_AVERAGE:
		for _, p := range points {
			point.Value += p.Value
		}
		point.Value /= float64(len(points))
	case AGGREGATION_SUM:
		for _, p := range points {
			point.Value += p.Value
		}
	case AGGREGATION_LAST:
		point.Value = points[len(points)-1].Value
	case AGGREGATION_MAX:
		point.Value = points[0].Value
		for _, p := range points {
			if p.Value > point.Value {
				point.Value = p.Value
			}
		}
	case AGGREGATION_MIN:
		point.Value = points[0].Value
		for _, p := range points {
			if p.Value < point.Value {
				point.Value = p.Value
			}
		}
	default:
		err = errors.New("unknown aggregation function")
	}
	return
}
