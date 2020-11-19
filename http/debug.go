package http

import (
	"fmt"
	"log"
)

var debugFlag bool

//
// Set debug log flag for client logging (of which there is little rght now)
func DebugFlag(debug bool) {
	debugFlag = debug
}

//
// Print a log message when debug flag is set
func Debug(format string, args ...interface{}) {
	if !debugFlag {
		return
	}
	msg := fmt.Sprintf(format, args...)
	log.Printf("[DBG: %s", msg)
}
