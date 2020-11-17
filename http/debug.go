package http

import (
	"fmt"
	"log"
)

var debugFlag bool

func DebugFlag(debug bool) {
	debugFlag = debug
}

func Debug(format string, args ...interface{}) {
	if !debugFlag {
		return
	}
	msg := fmt.Sprintf(format, args...)
	log.Printf("[DBG: %s", msg)
}
