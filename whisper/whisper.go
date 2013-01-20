/*

Package whisper implements an interface to the whisper database format used by the Graphite project (https://github.com/graphite-project/)

*/
package whisper

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Metadata holds metadata that's common to an entire whisper database
type Metadata struct {
	AggregationMethod AggregationMethod // Aggregation method used. See the AGGREGATION_* constants
	MaxRetention      uint32            // The maximum retention period
	XFilesFactor      float32           // The minimum percentage of known values required to aggregate
	ArchiveCount      uint32            // The number of archives in the database
}

// ArchiveInfo holds metadata about a single archive within a whisper database
type ArchiveInfo struct {
	Offset          uint32 // The byte offset of the archive within the database
	SecondsPerPoint uint32 // The number of seconds of elapsed time represented by a data point
	Points          uint32 // The number of data points
}

// Returns the retention period of the archive in seconds
func (a ArchiveInfo) Retention() uint32 {
	return a.SecondsPerPoint * a.Points
}

// Calculates the size of the archive in bytes
func (a ArchiveInfo) size() uint32 {
	return a.Points * pointSize
}

// Calculates byte offset of the last point in the archive
func (a ArchiveInfo) end() uint32 {
	return a.Offset + a.size()
}

type AggregationMethod uint32

// Valid aggregation methods
const (
	AGGREGATION_UNKNOWN AggregationMethod = 0 // Unknown aggregation method
	AGGREGATION_AVERAGE AggregationMethod = 1 // Aggregate using averaging
	AGGREGATION_SUM     AggregationMethod = 2 // Aggregate using sum
	AGGREGATION_LAST    AggregationMethod = 3 // Aggregate using the last value
	AGGREGATION_MAX     AggregationMethod = 4 // Aggregate using the maximum value
	AGGREGATION_MIN     AggregationMethod = 5 // Aggregate using the minimum value
)

func (a *AggregationMethod) String() (s string) {
	switch *a {
	case AGGREGATION_AVERAGE:
		s = "average"
	case AGGREGATION_SUM:
		s = "sum"
	case AGGREGATION_LAST:
		s = "last"
	case AGGREGATION_MIN:
		s = "min"
	case AGGREGATION_MAX:
		s = "max"
	default:
		s = "unknown"
	}
	return
}

func (a *AggregationMethod) Set(s string) error {
	switch s {
	case "average":
		*a = AGGREGATION_AVERAGE
	case "sum":
		*a = AGGREGATION_SUM
	case "last":
		*a = AGGREGATION_LAST
	case "min":
		*a = AGGREGATION_MIN
	case "max":
		*a = AGGREGATION_MAX
	default:
		*a = AGGREGATION_UNKNOWN
	}
	return nil
}

// Header contains all the metadata about a whisper database.
type Header struct {
	Metadata Metadata      // General metadata about the database
	Archives []ArchiveInfo // Information about each of the archives in the database, in order of precision
}

// A Point is a single datum stored in a whisper database.
type Point struct {
	Timestamp uint32  // Timestamp in seconds past the epoch
	Value     float64 // Data point value
}

// Interval repsents a time interval with a step.
type Interval struct {
	FromTimestamp  uint32 // Start of the interval in seconds since the epoch
	UntilTimestamp uint32 // End of the interval in seconds since the epoch
	Step           uint32 // Step size in seconds
}

// Whisper represents a handle to a whisper database.
type Whisper struct {
	Header Header
	file   *os.File
}

// Unexported members

// type for sorting a list of ArchiveInfo by the SecondsPerPoint field
type bySecondsPerPoint []ArchiveInfo

// sort.Interface
func (a bySecondsPerPoint) Len() int           { return len(a) }
func (a bySecondsPerPoint) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySecondsPerPoint) Less(i, j int) bool { return a[i].SecondsPerPoint < a[j].SecondsPerPoint }

// a list of points
type archive []Point

// sort.Interface
func (a archive) Len() int           { return len(a) }
func (a archive) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archive) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

// a type for reverse sorting of archives
type reverseArchive struct{ archive }

// sort.Interface
func (r reverseArchive) Less(i, j int) bool { return r.archive.Less(j, i) }

// some sizes used fo
var pointSize, metadataSize, archiveSize uint32

// a regular expression matching a precision string such as 120y
var precisionRegexp = regexp.MustCompile("^(\\d+)([smhdwy]?)")

func init() {
	pointSize = uint32(binary.Size(Point{}))
	metadataSize = uint32(binary.Size(Metadata{}))
	archiveSize = uint32(binary.Size(archive{}))
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
	err = binary.Read(buf, binary.BigEndian, &metadata)
	if err != nil {
		return
	}
	header.Metadata = metadata

	// Read archive info
	archives := make([]ArchiveInfo, metadata.ArchiveCount)
	for i := uint32(0); i < metadata.ArchiveCount; i++ {
		err = binary.Read(buf, binary.BigEndian, &archives[i])
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
func ValidateArchiveList(archives []ArchiveInfo) error {
	sort.Sort(bySecondsPerPoint(archives))

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
			return errors.New("no archive may be a duplicate of another")
		}

		// 3.
		if nextArchive.SecondsPerPoint%archive.SecondsPerPoint != 0 {
			return errors.New("higher precision archives must evenly divide in to lower precision")
		}

		// 4.
		nextRetention := nextArchive.Retention()
		retention := archive.Retention()
		if !(nextRetention > retention) {
			return errors.New("lower precision archives must cover a larger time interval than higher precision")
		}

		// 5.
		if !(archive.Points >= (nextArchive.SecondsPerPoint / archive.SecondsPerPoint)) {
			return errors.New("each archive must be able to consolidate the next")
		}

	}
	return nil

}

// Create a new whisper database at a given file path
func Create(path string, archives []ArchiveInfo, xFilesFactor float32, aggregationMethod AggregationMethod, sparse bool) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

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
	if err := binary.Write(file, binary.BigEndian, metadata); err != nil {
		return err
	}

	headerSize := metadataSize + (archiveSize * uint32(len(archives)))
	archiveOffsetPointer := headerSize

	for _, archive := range archives {
		archive.Offset = archiveOffsetPointer
		if err := binary.Write(file, binary.BigEndian, archive); err != nil {
			return err
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

	return nil
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
	var lowerArchives []ArchiveInfo
	var currentArchive ArchiveInfo
	for i, ca := range w.Header.Archives {
		if ca.Retention() < diff {
			continue
		}
		lowerArchives = w.Header.Archives[i+1:]
		currentArchive = ca
	}

	// Normalize the point's timestamp to the current archive's precision and write the point
	point.Timestamp = point.Timestamp - (point.Timestamp % currentArchive.SecondsPerPoint)
	err = w.writePoints(currentArchive, point)

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
	var currentPoints archive

PointLoop:
	for _, point := range points {
		age := now - point.Timestamp

		for currentArchive.Retention() < age {
			if len(currentPoints) > 0 {
				sort.Sort(reverseArchive{currentPoints})
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
		sort.Sort(reverseArchive{currentPoints})
		w.archiveUpdateMany(*currentArchive, currentPoints)
	}

	return
}

// Fetch all points since a timestamp
func (w Whisper) Fetch(from uint32) (interval Interval, points []Point, err error) {
	now := uint32(time.Now().Unix())
	return w.FetchUntil(from, now)
}

// Fetch all points between two timestamps
func (w Whisper) FetchUntil(from, until uint32) (interval Interval, points []Point, err error) {
	now := uint32(time.Now().Unix())

	// Tidy up the time ranges
	oldest := now - w.Header.Metadata.MaxRetention
	if from < oldest {
		from = oldest
	}
	if from > until {
		err = errors.New("from time is not less than until time")
	}
	if until > now {
		until = now
	}

	// Find the archive with enough retention to get be holding our data
	var archive ArchiveInfo
	diff := now - from
	for _, info := range w.Header.Archives {
		if info.Retention() >= diff {
			archive = info
			break
		}
	}

	step := archive.SecondsPerPoint
	fromTimestamp := quantizeTimestamp(from, step) + step
	fromOffset, err := w.pointOffset(archive, fromTimestamp)
	if err != nil {
		return
	}

	untilTimestamp := quantizeTimestamp(until, step) + step
	untilOffset, err := w.pointOffset(archive, untilTimestamp)
	if err != nil {
		return
	}

	points, err = w.readPointsBetweenOffsets(archive, fromOffset, untilOffset)
	interval = Interval{fromTimestamp, untilTimestamp, step}
	return
}

func (w Whisper) archiveUpdateMany(archiveInfo ArchiveInfo, points archive) (err error) {
	type stampedArchive struct {
		timestamp uint32
		points    archive
	}
	var archives []stampedArchive
	var currentPoints archive
	var previousTimestamp, archiveStart uint32

	step := archiveInfo.SecondsPerPoint
	points = quantizeArchive(points, step)

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
			currentPoints = archive{}
		}

		currentPoints = append(currentPoints, point)
		previousTimestamp = point.Timestamp

	}

	if len(currentPoints) > 0 {
		// If there are any more points remaining after the loop, make a new series for them as well
		archiveStart = previousTimestamp - (uint32(len(currentPoints)) * step)
		archives = append(archives, stampedArchive{archiveStart, currentPoints})
	}

	for _, archive := range archives {
		err = w.writePoints(archiveInfo, archive.points...)
		if err != nil {
			return err
		}
	}

	higher := archiveInfo

PropagateLoop:
	for _, info := range w.Header.Archives {
		if info.SecondsPerPoint < archiveInfo.SecondsPerPoint {
			continue
		}

		quantizedPoints := quantizeArchive(points, info.SecondsPerPoint)
		lastPoint := Point{0, 0}
		for _, point := range quantizedPoints {
			if point.Timestamp == lastPoint.Timestamp {
				continue
			}

			propagateFurther, err := w.propagate(point.Timestamp, higher, info)
			if err != nil {
				return err
			}
			if !propagateFurther {
				break PropagateLoop
			}

			lastPoint = point
		}
		higher = info
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
	relativeLastOffset := (relativeFirstOffset + higherPointsSize) % higher.size()

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

	err = w.writePoints(lower, aggregatePoint)

	return true, nil

}

// Set the aggregation method for the database
func (w Whisper) SetAggregationMethod(aggregationMethod AggregationMethod) error {
	//TODO: Validate the value of aggregationMethod
	w.Header.Metadata.AggregationMethod = aggregationMethod
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}
	return binary.Write(w.file, binary.BigEndian, w.Header.Metadata)
}

// Read a single point from an offset in the database
func (w Whisper) readPoint(offset uint32) (point Point, err error) {
	points := make([]Point, 1)
	err = w.readPoints(offset, points)
	point = points[0]
	return
}

// Read a slice of points from an offset in the database
func (w Whisper) readPoints(offset uint32, points []Point) error {
	_, err := w.file.Seek(int64(offset), 0)
	if err != nil {
		return err
	}
	return binary.Read(w.file, binary.BigEndian, points)
}

func (w Whisper) readPointsBetweenOffsets(archive ArchiveInfo, startOffset, endOffset uint32) (points []Point, err error) {
	archiveStart := archive.Offset
	archiveEnd := archive.end()
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

// Write a points to an archive in the order given
// The offset is determined by the first point
func (w Whisper) writePoints(archive ArchiveInfo, points ...Point) (err error) {
	nPoints := uint32(len(points))

	// Sanity check
	if nPoints > archive.Points {
		return errors.New(fmt.Sprintf("archive can store at most %d points, %d supplied",
			archive.Points, nPoints))
	}

	// Get the offset of the first point
	offset, err := w.pointOffset(archive, points[0].Timestamp)
	if err != nil {
		return
	}

	_, err = w.file.Seek(int64(offset), 0)
	if err != nil {
		return
	}

	maxPointsFromOffset := (archive.end() - offset) / pointSize
	if nPoints > maxPointsFromOffset {
		// Points span the beginning and end of the archive, eg: ##----###
		err = binary.Write(w.file, binary.BigEndian, points[:maxPointsFromOffset])
		if err != nil {
			return
		}

		_, err = w.file.Seek(int64(archive.Offset), 0)
		if err != nil {
			return
		}

		err = binary.Write(w.file, binary.BigEndian, points[maxPointsFromOffset:])
		if err != nil {
			return
		}
	} else {
		// Points are in the middle of the archive, eg: --####---
		binary.Write(w.file, binary.BigEndian, points)
	}

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
		offset = archive.Offset + (byteDistance % archive.size())
	}
	return
}

/*
ParseArchiveInfo returns an ArchiveInfo represented by the string.

The string must consist of two numbers, the precision and retention, separated by a colon (:).

Both the precision and retention strings accept a unit suffix. Acceptable suffixes are: "s" for second,
"m" for minute, "h" for hour, "d" for day, "w" for week, and "y" for year.

The precision string specifies how large of a time interval is represented by a single point in the archive.

The retention string specifies how long points are kept in the archive. If no suffix is given for the retention
it is taken to mean a number of points and not a duration.

*/
func ParseArchiveInfo(archiveString string) (a ArchiveInfo, err error) {
	c := strings.Split(archiveString, ":")
	if len(c) != 2 {
		err = errors.New(fmt.Sprintf("could not parse: %s", archiveString))
		return
	}

	precision := c[0]
	retention := c[1]

	parsedPrecision := precisionRegexp.FindStringSubmatch(precision)
	if parsedPrecision == nil {
		err = errors.New(fmt.Sprintf("invalid precision string: %s", precision))
		return
	}

	secondsPerPoint, err := parseUint32(parsedPrecision[1])
	if err != nil {
		return
	}

	if parsedPrecision[2] != "" {
		secondsPerPoint, err = expandUnits(secondsPerPoint, parsedPrecision[2])
		if err != nil {
			return
		}
	}

	parsedPoints := precisionRegexp.FindStringSubmatch(retention)
	if parsedPoints == nil {
		err = errors.New(fmt.Sprintf("invalid retention string: %s", precision))
		return
	}

	points, err := parseUint32(parsedPoints[1])
	if err != nil {
		return
	}

	var retentionSeconds uint32
	if parsedPoints[2] != "" {
		retentionSeconds, err = expandUnits(points, parsedPoints[2])
		if err != nil {
			return
		}
		points = retentionSeconds / secondsPerPoint
	}

	a = ArchiveInfo{0, secondsPerPoint, points}
	return
}

func quantizeArchive(points archive, resolution uint32) archive {
	result := archive{}
	for _, point := range points {
		result = append(result, Point{quantizeTimestamp(point.Timestamp, resolution), point.Value})
	}
	return result
}

func quantizeTimestamp(timestamp uint32, resolution uint32) (quantized uint32) {
	return timestamp - (timestamp % resolution)
}

func aggregate(aggregationMethod AggregationMethod, points []Point) (point Point, err error) {
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
