package whisper

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var ainfo = NewArchiveInfo

func tempFileName() string {
	f, err := ioutil.TempFile("", "whisper")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func TestQuantizeArchive(t *testing.T) {
	points := archive{Point{0, 0}, Point{3, 0}, Point{10, 0}}
	pointsOut := archive{Point{0, 0}, Point{2, 0}, Point{10, 0}}
	quantizedPoints := quantizeArchive(points, 2)
	for i := range quantizedPoints {
		if quantizedPoints[i] != pointsOut[i] {
			t.Errorf("%v != %v", quantizedPoints[i], pointsOut[i])
		}
	}
}

func TestQuantizePoint(t *testing.T) {
	var pointTests = []struct {
		in         uint32
		resolution uint32
		out        uint32
	}{
		{0, 2, 0},
		{3, 2, 2},
	}

	for i, tt := range pointTests {
		q := quantizeTimestamp(tt.in, tt.resolution)
		if q != tt.out {
			t.Errorf("%d. quantizePoint(%q, %q) => %q, want %q", i, tt.in, tt.resolution, q, tt.out)
		}
	}
}

func TestAggregate(t *testing.T) {
	points := archive{Point{0, 0}, Point{0, 1}, Point{0, 2}, Point{0, 1}}
	expected := Point{0, 1}
	if p, err := aggregate(AggregationAverage, points); (p != expected) || (err != nil) {
		t.Errorf("Average failed to average to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 4}
	if p, err := aggregate(AggregationSum, points); (p != expected) || (err != nil) {
		t.Errorf("Sum failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 1}
	if p, err := aggregate(AggregationLast, points); (p != expected) || (err != nil) {
		t.Errorf("Last failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 2}
	if p, err := aggregate(AggregationMax, points); (p != expected) || (err != nil) {
		t.Errorf("Max failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 0}
	if p, err := aggregate(AggregationMin, points); (p != expected) || (err != nil) {
		t.Errorf("Min failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	if _, err := aggregate(1000, points); err == nil {
		t.Errorf("No error for invalid aggregation")
	}
}

func TestParseArchiveInfo(t *testing.T) {
	tests := map[string]ArchiveInfo{
		"60:1440": ArchiveInfo{0, 60, 1440},    // 60 seconds per datapoint, 1440 datapoints = 1 day of retention
		"15m:8":   ArchiveInfo{0, 15 * 60, 8},  // 15 minutes per datapoint, 8 datapoints = 2 hours of retention
		"1h:7d":   ArchiveInfo{0, 3600, 168},   // 1 hour per datapoint, 7 days of retention
		"12h:2y":  ArchiveInfo{0, 43200, 1456}, // 12 hours per datapoint, 2 years of retention
	}

	for info, expected := range tests {
		if a, err := ParseArchiveInfo(info); (a != expected) || (err != nil) {
			t.Errorf("%s: %v != %v, %v", info, a, expected, err)
		}
	}

}

func TestWhisperAggregation(t *testing.T) {
	filename := tempFileName()
	defer os.Remove(filename)
	w, err := Create(filename, []ArchiveInfo{}, 0.5, AggregationMin, false)
	if err != nil {
		t.Fatal("failed to create database:", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close database:", err)
		}
	}()

	w.SetAggregationMethod(AggregationMax)
	if method := w.Header.Metadata.AggregationMethod; method != AggregationMax {
		t.Fatalf("AggregationMethod: %d, want %d", method, AggregationMax)
	}
}

func TestArchiveHeader(t *testing.T) {
	filename := tempFileName()
	defer os.Remove(filename)

	w, err := Create(filename, []ArchiveInfo{ainfo(1, 60), ainfo(60, 60)}, 0.5, AggregationAverage, false)
	if err != nil {
		t.Fatal("failed to create database:", err)
	}

	verifyHeader := func(w *Whisper) {
		meta := w.Header.Metadata
		expectedMeta := Metadata{AggregationAverage, 60 * 60, 0.5, 2}
		if meta != expectedMeta {
			t.Errorf("bad metadata, got %v want %v", meta, expectedMeta)
		}

		archive0 := ArchiveInfo{metadataSize, 1, 60}
		if w.Header.Archives[0] != archive0 {
			t.Errorf("bad archive 0, got %v want %v", w.Header.Archives[0], archive0)
		}

		archive1 := ArchiveInfo{metadataSize + pointSize*60, 60, 60}
		if w.Header.Archives[1] != archive1 {
			t.Errorf("bad archive 1, got %v want %v", w.Header.Archives[1], archive1)
		}
	}

	verifyHeader(w)
	if err := w.Close(); err != nil {
		t.Fatal("failed to close database:", err)
	}

	w, err = Open(filename)
	if err != nil {
		t.Fatal("failed to open database:", err)
	}
	verifyHeader(w)
	if err := w.Close(); err != nil {
		t.Fatal("failed to close database:", err)
	}
}

func TestMaxRetention(t *testing.T) {
	filename := tempFileName()
	defer os.Remove(filename)

	w, err := Create(filename, []ArchiveInfo{NewArchiveInfo(60, 10)}, 0.5, AggregationAverage, false)
	if err != nil {
		t.Fatal("failed to create database:", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Fatal("failed to close database:", err)
		}
	}()

	invalid := NewPoint(time.Now().Add(-11*time.Minute), 0)
	if err = w.Update(invalid); err == nil {
		t.Fatal("invalid point did not return an error")
	}
	valid := NewPoint(time.Now().Add(-9*time.Minute), 0)
	if err = w.Update(valid); err != nil {
		t.Fatalf("valid point returned an error: %s", err)
	}
}

func TestCreateTwice(t *testing.T) {
	filename := tempFileName()
	archiveInfos := []ArchiveInfo{NewArchiveInfo(60, 10)}
	defer os.Remove(filename)

	w, err := Create(filename, archiveInfos, 0.5, AggregationAverage, false)
	if err != nil {
		t.Fatal("failed to create database:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("failed to close database:", err)
	}

	_, err = Create(filename, archiveInfos, 0.5, AggregationAverage, false)
	if err == nil {
		t.Fatal("no error when attempting to overwrite database")
	}
}

func TestValidateArchiveList(t *testing.T) {
	tests := []struct {
		Archives []ArchiveInfo
		Error    error
	}{
		{[]ArchiveInfo{}, ErrNoArchives},
		{[]ArchiveInfo{ainfo(10, 10), ainfo(10, 5)}, ErrDuplicateArchive},
		{[]ArchiveInfo{ainfo(2, 5), ainfo(3, 5)}, ErrUnevenPrecision},
		{[]ArchiveInfo{ainfo(10, 6), ainfo(5, 13)}, ErrLowRetention},
		{[]ArchiveInfo{ainfo(10, 6), ainfo(70, 10)}, ErrInsufficientPoints},
		{[]ArchiveInfo{ainfo(2, 5), ainfo(4, 10), ainfo(8, 20)}, nil},

		// The following tests adapted from test_whisper.py
		{[]ArchiveInfo{ainfo(1, 60), ainfo(60, 60)}, nil},
		{[]ArchiveInfo{ainfo(1, 60), ainfo(60, 60), ainfo(1, 60)}, ErrDuplicateArchive},
		{[]ArchiveInfo{ainfo(60, 60), ainfo(6, 60)}, nil},
		{[]ArchiveInfo{ainfo(60, 60), ainfo(7, 60)}, ErrUnevenPrecision},
		{[]ArchiveInfo{ainfo(1, 60), ainfo(10, 1)}, ErrLowRetention},
		{[]ArchiveInfo{ainfo(1, 30), ainfo(60, 60)}, ErrInsufficientPoints},
	}

	for i, test := range tests {
		if err := ValidateArchiveList(test.Archives); err != test.Error {
			t.Errorf("%d: got: %v, want: %v", i, err, test.Error)
		}
	}
}
