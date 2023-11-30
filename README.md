# gazelle
Dataframe library in Go, WIP.

This library is built off of Go Generics, but due to the limitations of go, and wanting to limit the use of interfaces, the api will be a little different then what you may be used to.

I.E

if you had a series in pandas, you may be used to saying:

s.sum() -> 15

in this library this would look like:

s := series.NewSeries()

series.sum(s) -> 15