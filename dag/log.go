package dag

import (
	"time"
	"fmt"
)

func (d *DAG) logLine(level, msg string) {
	line := fmt.Sprintf("[%s] [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), level, msg)
	d.logLock.Lock()
	defer d.logLock.Unlock()
	d.logFile.WriteString(line)
}

func (d *DAG) Logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	d.logLine("INFO", msg)
}

func (d *DAG) LogErrorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	d.logLine("ERROR", msg)
}