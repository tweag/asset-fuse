package logging

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarning
	LogLevelBasic
	LogLevelDebug
)

var level = LogLevelBasic

func SetLevel(l LogLevel) {
	level = l
}

func GetLevel() LogLevel {
	return level
}

func FromString(s string) LogLevel {
	if numericLogLevel, err := strconv.Atoi(s); err == nil {
		return boundedLogLevel(numericLogLevel)
	}
	switch strings.ToLower(s) {
	case "error":
		return LogLevelError
	case "warning":
		return LogLevelWarning
	case "basic":
		return LogLevelBasic
	case "debug":
		return LogLevelDebug
	}

	return LogLevelBasic
}

func Debugf(format string, args ...any) {
	if level >= LogLevelDebug {
		fPrintStderr(format, args...)
	}
}

func Warningf(format string, args ...any) {
	if level >= LogLevelWarning {
		fPrintStderr(format, args...)
	}
}

func Basicf(format string, args ...any) {
	if level >= LogLevelBasic {
		fPrintStderr(format, args...)
	}
}

func Errorf(format string, args ...any) {
	fPrintStderr(format, args...)
}

func Fatalf(format string, args ...any) {
	fPrintStderr(format, args...)
	os.Exit(1)
}

func boundedLogLevel(numericLevel int) LogLevel {
	if numericLevel < 0 {
		return LogLevelError
	}
	if numericLevel > 3 {
		return LogLevelDebug
	}
	return LogLevel(numericLevel)
}

func fPrintStderr(format string, args ...any) {
	fmt.Fprint(os.Stderr, fmtWithNewline(format, args...))
}

func fmtWithNewline(format string, args ...any) string {
	out := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(out, "\n") {
		return out + "\n"
	}
	return out
}
