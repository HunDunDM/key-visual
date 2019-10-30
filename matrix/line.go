package matrix

type Value interface {
	Split(count int) Value         // split the value into count ones
	Merge(other Value)             // merge other value into self
	Useless(threshold uint64) bool // check if this value is a low-info at certain threshold, used at merging
	GetThreshold() uint64          // get the threshold
	Clone() Value                  // clone the value
	Reset()                        // reset the value to 0
	Default() Value                // generate a living example of initial value 0
	Equal(other Value) bool        // check if two values are equal
}

type Line struct {
	EndKey string `json:"end_key"`
	Value  `json:"value"`
}
