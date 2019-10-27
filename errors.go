package main

import (
	"fmt"
	"os"
	"runtime/debug"
)

func perr(err error) {
	if err == nil {
		return
	}
	fmt.Println(err.Error())
	debug.PrintStack()
	os.Exit(1)
}
