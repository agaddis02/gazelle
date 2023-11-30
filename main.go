package main

import (
	"github.com/agaddis02/gazelle/series"
)

func main() {
	intSeries := series.NewSeries("IntSeries", []int64{1, 2, 3, 4, 5})
	// floatSeries := series.NewSeries("FloatSeries", []float64{1.1, 2.2, 3.3})

	println(series.Sum(*intSeries))
	println(series.Average(*intSeries))
	println(series.Min(*intSeries))
	println(intSeries.Data.String())
	intSeries.Print()
}
