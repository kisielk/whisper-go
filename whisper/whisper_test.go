package whisper

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

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
	if p, err := aggregate(AGGREGATION_AVERAGE, points); (p != expected) || (err != nil) {
		t.Errorf("Average failed to average to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 4}
	if p, err := aggregate(AGGREGATION_SUM, points); (p != expected) || (err != nil) {
		t.Errorf("Sum failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 1}
	if p, err := aggregate(AGGREGATION_LAST, points); (p != expected) || (err != nil) {
		t.Errorf("Last failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 2}
	if p, err := aggregate(AGGREGATION_MAX, points); (p != expected) || (err != nil) {
		t.Errorf("Max failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0, 0}
	if p, err := aggregate(AGGREGATION_MIN, points); (p != expected) || (err != nil) {
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
	w, err := Create(filename, []ArchiveInfo{}, 0.5, AGGREGATION_MIN, false)
	if err != nil {
		panic(err)
	}

	w.SetAggregationMethod(AGGREGATION_MAX)
	if method := w.Header.Metadata.AggregationMethod; method != AGGREGATION_MAX {
		t.Fatalf("AggregationMethod: %d, want %d", method, AGGREGATION_MAX)
	}
}

func TestMaxRetention(t *testing.T) {
	filename := tempFileName()
	w, err := Create(filename, []ArchiveInfo{ArchiveInfo{SecondsPerPoint: 60, Points: 10}}, 0.5, AGGREGATION_AVERAGE, false)
	if err != nil {
		panic(err)
	}

	invalid := Point{uint32(time.Now().Add(-11 * time.Minute).Unix()), 0}
	if err = w.Update(invalid); err == nil {
		t.Fatal("invalid point did not return an error")
	}
	valid := Point{uint32(time.Now().Add(-9 * time.Minute).Unix()), 0}
	if err = w.Update(valid); err != nil {
		t.Fatalf("valid point returned an error: %s", err)
	}
}
