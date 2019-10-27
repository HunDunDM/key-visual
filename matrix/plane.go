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
type Label struct {
	StartKey string    `json:"start_key"`
	EndKey   string    `json:"end_key"`
	Names    []*string `json:"labels"`
}
type Matrix struct {
	Data   [][]Value     `json:"data"`   // 二维数据图
	Keys   DiscreteKeys  `json:"keys"`   // 纵坐标
	Times  DiscreteTimes `json:"times"`  // 横坐标
	Labels []*Label      `json:"labels"` // 标签信息
}

// 获取离散化后的time序列，含StartTime
func (plane *DiscretePlane) GetDiscreteTimes() DiscreteTimes {
	discreteTimes := make(DiscreteTimes, 0, len(plane.Axes)+1)
	discreteTimes = append(discreteTimes, plane.StartTime)
	for _, axis := range plane.Axes {
		if axis != nil { //实际应该不会出现
			discreteTimes = append(discreteTimes, axis.EndTime)
		}
	}
	return discreteTimes
}

// 把多个时间上连续的key轴压缩为一个key轴
func (plane *DiscretePlane) Compact() (axis *DiscreteAxis, startTime time.Time) {
	startTime = plane.StartTime
	axis = new(DiscreteAxis)
	length := len(plane.Axes)
	axis.EndTime = plane.Axes[length-1].EndTime
	// 把Plane里面每条key轴的keys都拿出来放在一个string切片里
	// isInside用于判断一个Key是否已经放在切片中了
	isInside := make(map[string]bool, 0)
	allKeys := make([]string, 0)
	for _, d := range plane.Axes {
		if len(d.Lines) == 0 {
			//忽略空的key轴
			continue
		}
		for _, k := range d.GetDiscreteKeys() {
			_, ok := isInside[k]
			if ok == false {
				isInside[k] = true
				allKeys = append(allKeys, k)
			}
		}
	}
	sort.Strings(allKeys)
	//初始一个Value, 用于初始化
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
	if len(allKeys) == 0 {
		//此时Compact是个空轴，StartKey实际无意义
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

// 给定推荐值n、m，将稀疏平面像素化为近似n*m的矩阵
func (plane *DiscretePlane) Pixel(n int, m int) *Matrix {
	newPlane := DiscretePlane{
		StartTime: plane.StartTime,
	}
	if n == 0 || m == 0 {
		return nil
	}
	//时间轴上，均分压缩
	if len(plane.Axes) >= n {
		//压缩分为两部走，前一部分step较大（大1），后一部分step较小
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
			//将step个key轴合并
			tempPlane := &DiscretePlane{}
			if i == 0 {
				tempPlane.StartTime = plane.StartTime
			} else {
				tempPlane.StartTime = plane.Axes[index-1].EndTime
			}
			tempPlane.Axes = make([]*DiscreteAxis, step)
			for i := 0; i < step; i++ {
				tempPlane.Axes[i] = plane.Axes[index+i].Clone()
			}
			axis, _ := tempPlane.Compact()

			//将合并后的key插入newPlane
			newPlane.Axes = append(newPlane.Axes, axis)
		}
	} else {
		newPlane.Axes = make([]*DiscreteAxis, len(plane.Axes))
		for i := 0; i < len(plane.Axes); i++ {
			newPlane.Axes[i] = plane.Axes[i].Clone()
		}
	}

	//生成统一的key轴
	axis, _ := newPlane.Compact()
	//对key轴压缩
	if len(axis.Lines) > m {
		//压缩处理，将key轴line的个数压缩到接近m
		thresholdSet := make(map[uint64]struct{}, len(axis.Lines))
		//去重
		for _, line := range axis.Lines {
			thresholdSet[line.GetThreshold()] = struct{}{}
		}

		thresholds := make([]uint64, 0, len(thresholdSet))
		for threshold := range thresholdSet {
			thresholds = append(thresholds, threshold)
		}
		sort.Slice(thresholds, func(i, j int) bool { return thresholds[i] < thresholds[j] })
		//二分查找
		i := sort.Search(len(thresholds), func(i int) bool {
			return axis.Effect(thresholds[i]) <= uint(m)
		})

		//取最相近的
		threshold1 := thresholds[i]
		num1 := axis.Effect(threshold1)
		if i > 0 && num1 != uint(m) {
			threshold2 := thresholds[i-1]
			num2 := axis.Effect(threshold2)
			if (int(num2) - m) < (m - int(num1)) {
				axis.DeNoise(threshold2)
			} else {
				axis.DeNoise(threshold1)
			}
		} else {
			axis.DeNoise(threshold1)
		}
	}

	//重置目标轴的value为0值
	for i := 0; i < len(axis.Lines); i++ {
		axis.Lines[i].Reset()
	}
	for i := 0; i < len(newPlane.Axes); i++ {
		axisClone := axis.Clone()
		newPlane.Axes[i].DeProjection(axisClone)
		axisClone.EndTime = newPlane.Axes[i].EndTime
		newPlane.Axes[i] = axisClone
	}

	//生成Matrix
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
