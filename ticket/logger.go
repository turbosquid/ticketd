package ticket

import (
	"fmt"
	"log"
)

//
// Logging interface. If you want to create your own logger, be sure to conform to this interface
// You can pass a logger to ticket.CreateTicketD
type Logger interface {
	Log(level int, fmtstr string, v ...interface{})
}

//
// Default logger. Create with the desired log level
type DefaultLogger struct {
	Level int
}

func (l *DefaultLogger) Log(level int, fmtstr string, v ...interface{}) {
	if level > l.Level {
		return
	}
	msg := fmt.Sprintf(fmtstr, v...)
	log.Printf("[%d] %s", level, msg)
}
