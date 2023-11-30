package series

import (
	"fmt"
	"os"
	"reflect"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/jedib0t/go-pretty/v6/table"
)

// Series is a generic type for series data
type Series[T any] struct {
	Data  arrow.Array
	Name  string
	Dtype arrow.DataType
}

// NewSeries creates a new series of a given data type
func NewSeries[T any](name string, values []T) *Series[T] {
	pool := memory.NewGoAllocator()
	var arr arrow.Array

	// Determine the type of the elements in the values slice
	kind := reflect.TypeOf((*T)(nil)).Elem().Kind()

	switch kind {
	case reflect.Int, reflect.Int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		for _, v := range values {
			val := reflect.ValueOf(v).Int()
			b.Append(int64(val))
		}
		arr = b.NewArray()
	case reflect.Float32, reflect.Float64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		for _, v := range values {
			val := reflect.ValueOf(v).Float()
			b.Append(val)
		}
		arr = b.NewArray()
	case reflect.String:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		for _, v := range values {
			val := reflect.ValueOf(v).String()
			b.Append(val)
		}
		arr = b.NewArray()
	// Add cases for other supported types
	default:
		panic("unsupported or unimplemented data type for Apache Arrow array")
	}

	return &Series[T]{
		Data:  arr,
		Name:  name,
		Dtype: arr.DataType(),
	}
}

func (s *Series[T]) GetName() string {
	return s.Name
}

func (s *Series[T]) GetDataType() arrow.DataType {
	return s.Dtype
}

func (s *Series[T]) GetData() arrow.Array {
	return s.Data
}

func (s *Series[T]) Print() {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Index", "Value"})

	const maxDisplay = 10 // Maximum number of rows to display

	var displayCount int
	switch arr := s.Data.(type) {
	case *array.Int64:
		displayCount = min(arr.Len(), maxDisplay)
		for i := 0; i < displayCount; i++ {
			t.AppendRow(table.Row{i, arr.Value(i)})
		}
	case *array.Float64:
		displayCount = min(arr.Len(), maxDisplay)
		for i := 0; i < displayCount; i++ {
			t.AppendRow(table.Row{i, arr.Value(i)})
		}
		// Add cases for other types as necessary
	}

	if displayCount < s.Data.Len() {
		t.AppendFooter(table.Row{"", fmt.Sprintf("... Length: %d, dtype: %s", s.Data.Len(), s.Data.DataType())})
	}

	t.Render()
}
