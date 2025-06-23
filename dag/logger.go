package dag

import (
	"log"
	"os"
)

type logger struct {
	infoLogger *log.Logger
}

var Logger *logger

func init() {
	file, err := os.OpenFile("dag.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	Logger = &logger{
		infoLogger: log.New(file, "[INFO] ", log.LstdFlags),
	}
}

func (l *logger) Infof(format string, v ...interface{}) {
	log.Printf(format, v...)
	l.infoLogger.Printf(format, v...)
}
