package internal

type Numbers interface {
	uint | int | int8 | int32 | int64 | uint8 | uint32 | uint64 | float32 | float64
}

func NearestMultiple[T Numbers](j, k T) T {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + T(1)) / k) * k
}
