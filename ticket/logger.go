package ticket

import (
	"fmt"
	"log"
)

type Logger interface {
	Log(level int, fmtstr string, v ...interface{})
}

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
