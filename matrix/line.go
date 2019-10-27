package matrix

type Value interface {
	Split(count int) Value         // 将值分裂为count个值
	Merge(other Value)             // 将other合并到self中
	Useless(threshold uint64) bool // 在给定阈值下是否为低信息量数据，用于合并
	GetThreshold() uint64          // 获得阈值
	Clone() Value                  // 克隆
	Reset()                        // 重置为0值
	Default() Value                // 生成一个初值为0的实例
	Equal(other Value) bool        // 判断是否相等
}

type Line struct {
	// StartKey string // EndKey from the previous Line
	EndKey string
	Value
}
