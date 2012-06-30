package whisper

import (
	"testing"
)


func TestQuantizeArchive(t *testing.T) {
	points := Archive{Point{0,0}, Point{3,0}, Point{10,0}}
	pointsOut := Archive{Point{0,0}, Point{2,0}, Point{10,0}}
	quantizeArchive(points, 2)
	for i := range points {
		if points[i] != pointsOut[i] {
			t.Errorf("%v != %v", points[i], pointsOut[i])
		}
	}
}

func TestAggregate(t *testing.T) {
	points := Archive{Point{0,0}, Point{0,1}, Point{0,2}, Point{0,1}}
	expected := Point{0,1}
	if p, err := aggregate(AGGREGATION_AVERAGE, points); (p != expected) || (err != nil) {
		t.Errorf("Average failed to average to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0,4}
	if p, err := aggregate(AGGREGATION_SUM, points); (p != expected) || (err != nil) {
		t.Errorf("Sum failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0,1}
	if p, err := aggregate(AGGREGATION_LAST, points); (p != expected) || (err != nil) {
		t.Errorf("Last failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0,2}
	if p, err := aggregate(AGGREGATION_MAX, points); (p != expected) || (err != nil) {
		t.Errorf("Max failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	expected = Point{0,0}
	if p, err := aggregate(AGGREGATION_MIN, points); (p != expected) || (err != nil) {
		t.Errorf("Min failed to aggregate to %v, got %v: %v", expected, p, err)
	}

	if _, err := aggregate(1000, points); err == nil {
		t.Errorf("No error for invalid aggregation")
	}
}

