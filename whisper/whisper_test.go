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
