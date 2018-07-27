package main

import (
	"fmt"

	"github.com/mlboy/gendry/builder"
)

func main() {
	v := builder.NewSQLSegment()
	v.Table("test")
	s := v.BuildSelect()
	fmt.Println(s)
}
