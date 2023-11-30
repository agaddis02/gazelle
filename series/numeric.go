package series

import (
	"math"

	"sort"

	"github.com/apache/arrow/go/v14/arrow/array"
)

type Numeric interface {
	~int | ~int64 | ~float32 | ~float64
	// Add other numeric types as needed
}

func Sum[T Numeric](s Series[T]) T {
	var sum T

	switch arr := s.Data.(type) {
	case *array.Int64:
		for i := 0; i < arr.Len(); i++ {
			sum += T(arr.Value(i))
		}
	case *array.Float64:
		for i := 0; i < arr.Len(); i++ {
			sum += T(arr.Value(i))
		}

	default:
		panic("unsupported or unimplemented data type")
	}

	return sum
}

func Average[T Numeric](s Series[T]) T {
	var sum T

	switch arr := s.Data.(type) {
	case *array.Int64:
		for i := 0; i < arr.Len(); i++ {
			sum += T(arr.Value(i))
		}
	case *array.Float64:
		for i := 0; i < arr.Len(); i++ {
			sum += T(arr.Value(i))
		}

	default:
		panic("unsupported or unimplemented data type")
	}

	return sum / T(s.Data.Len())
}

func Min[T Numeric](s Series[T]) T {
	var min T

	switch arr := s.Data.(type) {
	case *array.Int64:
		min = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			min = T(math.Min(float64(min), float64(arr.Value(i))))
		}
	case *array.Float64:
		min = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			min = T(math.Min(float64(min), arr.Value(i)))
		}
	default:
		panic("unsupported or unimplemented data type")
	}

	return min
}

func Max[T Numeric](s Series[T]) T {
	var max T

	switch arr := s.Data.(type) {
	case *array.Int64:
		max = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			max = T(math.Max(float64(max), float64(arr.Value(i))))
		}
	case *array.Float64:
		max = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			max = T(math.Max(float64(max), arr.Value(i)))
		}
	default:
		panic("unsupported or unimplemented data type")
	}

	return max
}
func Abs[T Numeric](s Series[T]) T {
	var abs T

	switch arr := s.Data.(type) {
	case *array.Int64:
		abs = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			abs = T(math.Abs(float64(abs)))
		}
	case *array.Float64:
		abs = T(arr.Value(0))
		for i := 1; i < arr.Len(); i++ {
			abs = T(math.Abs(float64(abs)))
		}
	default:
		panic("unsupported or unimplemented data type")
	}

	return abs
}

func Median[T Numeric](s Series[T]) T {
	var values []T

	// Extract values from the Arrow array and append to the Go slice
	switch arr := s.Data.(type) {
	case *array.Int64:
		for i := 0; i < arr.Len(); i++ {
			values = append(values, T(arr.Value(i)))
		}
	case *array.Float64:
		for i := 0; i < arr.Len(); i++ {
			values = append(values, T(arr.Value(i)))
		}
	// Add cases for other numeric types
	default:
		panic("unsupported or unimplemented data type")
	}

	// Sort the slice
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	// Find the median
	n := len(values)
	if n == 0 {
		panic("cannot calculate median of empty series")
	}
	if n%2 == 1 {
		return values[n/2]
	}
	return (values[n/2-1] + values[n/2]) / 2
}
