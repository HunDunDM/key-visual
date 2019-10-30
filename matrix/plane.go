package matrix

import (
	"sort"
	"time"
)

type DiscretePlane struct {
	StartTime time.Time // 第一条Axis的StartTime
	Axes      []*DiscreteAxis
}

type DiscreteTimes []time.Time

type Matrix struct {
	Data  [][]Value     `json:"data"`  // two-dimension data map
	Keys  DiscreteKeys  `json:"keys"`  // Y-axis of matrix
	Times DiscreteTimes `json:"times"` // X-axis of matrix
}

// get the time sets after discretization, including StartTime
func (plane *DiscretePlane) GetDiscreteTimes() DiscreteTimes {
	discreteTimes := make(DiscreteTimes, 0, len(plane.Axes)+1)
	discreteTimes = append(discreteTimes, plane.StartTime)
	for _, axis := range plane.Axes {
		if axis != nil {
			discreteTimes = append(discreteTimes, axis.EndTime)
		}
	}
	return discreteTimes
}

// compress consecutive key axises of different time into one key axis
func (plane *DiscretePlane) Compact() (axis *DiscreteAxis, startTime time.Time) {
	startTime = plane.StartTime
	axis = new(DiscreteAxis)
	length := len(plane.Axes)
	if length == 0 {
		return axis, startTime
	}
	axis.EndTime = plane.Axes[length-1].EndTime
	// keysSet is used to remove duplication
	keysSet := make(map[string]struct{}, len(plane.Axes[0].Lines))
	for _, axis := range plane.Axes {
		if len(axis.Lines) == 0 {
			// ignore empty key axis
			continue
		}
		keysSet[axis.StartKey] = struct{}{}
		for _, line := range axis.Lines {
			keysSet[line.EndKey] = struct{}{}
		}
	}

	allKeys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)
	var defaultValue Value
	for _, axis := range plane.Axes {
		for _, Line := range axis.Lines {
			defaultValue = Line.Default()
			break
		}
		if defaultValue != nil {
			break
		}
	}
	if defaultValue == nil {
		return axis, startTime
	}
	if len(allKeys) == 0 {
		// here StartKey is an empty key axis with no meaning
		axis.StartKey = ""
	} else {
		axis.StartKey = allKeys[0]
	}

	length = len(allKeys)
	if length > 0 {
		axis.Lines = make([]*Line, 0, length-1)
	}
	for i := 1; i < length; i++ {
		newLine := &(Line{
			EndKey: allKeys[i],
			Value:  defaultValue.Default(),
		})
		axis.Lines = append(axis.Lines, newLine)
	}
	for _, ax := range plane.Axes {
		ax.ReSample(axis)
	}
	return axis, startTime
}

// compress the time axises into 'n' ones
func (plane *DiscretePlane) TimesSquash(n int) *DiscretePlane {
	if n == 0 {
		return nil
	}
	newPlane := &DiscretePlane{
		StartTime: plane.StartTime,
	}

	// divide time axises equally and then compress
	if len(plane.Axes) >= n {
		// compression are two processes, the first one has a bigger step, the next one has a smaller step
		step2 := len(plane.Axes) / n
		step1 := step2 + 1
		n1 := len(plane.Axes) % n
		var index int
		var step int

		for i := 0; i < n; i++ {
			if i < n1 {
				step = step1
				index = i * step1
			} else {
				step = step2
				index = n1*step1 + (i-n1)*step2
			}
			// merge the step key axises
			tempPlane := &DiscretePlane{}
			if i == 0 {
				tempPlane.StartTime = plane.StartTime
			} else {
				tempPlane.StartTime = plane.Axes[index-1].EndTime
			}
			tempPlane.Axes = make([]*DiscreteAxis, step)
			for i := 0; i < step; i++ {
				tempPlane.Axes[i] = plane.Axes[index+i]
			}
			axis, _ := tempPlane.Compact()
			// insert the key after merge into newPlane
			newPlane.Axes = append(newPlane.Axes, axis)
		}
	} else {
		newPlane.Axes = make([]*DiscreteAxis, len(plane.Axes))
		for i := 0; i < len(plane.Axes); i++ {
			newPlane.Axes[i] = plane.Axes[i]
		}
	}
	return newPlane
}

// pixel the plane into a n*m matrix at the given n and m
func (plane *DiscretePlane) Pixel(n int, m int) *Matrix {
	if n == 0 || m == 0 {
		return nil
	}
	// compress on the time axises
	newPlane := plane.TimesSquash(n)

	// generate a united key axis
	axis, _ := newPlane.Compact()
	axis.BinaryCompress(m)

	// reset destination axis's value into 0
	for i := 0; i < len(axis.Lines); i++ {
		axis.Lines[i].Reset()
	}
	// for each key axis, do projection
	for i := 0; i < len(newPlane.Axes); i++ {
		axisClone := axis.Clone()
		newPlane.Axes[i].DeProjection(axisClone)
		axisClone.EndTime = newPlane.Axes[i].EndTime
		newPlane.Axes[i] = axisClone
	}

	// generate matrix
	discreteTimes := newPlane.GetDiscreteTimes()
	discreteKeys := axis.GetDiscreteKeys()
	timesLen := len(discreteTimes) - 1
	keysLen := len(discreteKeys) - 1
	matrix := &Matrix{
		Data:  make([][]Value, timesLen),
		Keys:  discreteKeys,
		Times: discreteTimes,
	}

	for i := 0; i < timesLen; i++ {
		matrix.Data[i] = make([]Value, keysLen)
		for j := 0; j < keysLen; j++ {
			matrix.Data[i][j] = newPlane.Axes[i].Lines[j].Value
		}
	}
	return matrix
}
