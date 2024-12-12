package logger

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
)

type DefaultLoggerFactory struct {
	hostname     string
	Filename     string
	MaxSizeMB    int
	MaxAgeDays   int
	MaxBackups   int
	Compress     bool
	LogToConsole bool
}

type LogrusHook struct {
	Hostname string
	Context  string
}

func (hook *LogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook *LogrusHook) Fire(entry *logrus.Entry) error {
	entry.Data["host"] = hook.Hostname
	entry.Data["context"] = hook.Context
	return nil
}

func GetLogFactory(
	filename string,
	maxSizeMB int,
	maxAgeDays int,
	maxBackups int,
	compress bool,
	logToConsole bool,
) (*DefaultLoggerFactory, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &DefaultLoggerFactory{
		Filename:     filename,
		MaxSizeMB:    maxSizeMB,
		MaxAgeDays:   maxAgeDays,
		MaxBackups:   maxBackups,
		Compress:     compress,
		LogToConsole: logToConsole,
		hostname:     hostname,
	}, nil
}

func (d *DefaultLoggerFactory) GetLogger(name string) *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.AddHook(&LogrusHook{
		Hostname: d.hostname,
		Context:  name,
	})
	if d.Filename != "" {
		out := &lumberjack.Logger{
			Filename:   d.Filename,
			MaxSize:    d.MaxSizeMB,
			MaxBackups: d.MaxBackups,
			MaxAge:     d.MaxAgeDays,
			Compress:   d.Compress,
		}
		if d.LogToConsole {
			logger.SetOutput(io.MultiWriter(os.Stdout, out))
		} else {
			logger.SetOutput(out)
		}
	}
	return logger
}
