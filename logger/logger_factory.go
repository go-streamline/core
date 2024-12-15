package logger

import (
	"github.com/go-streamline/interfaces/definitions"
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
	Level        logrus.Level
	CustomLevels map[string]logrus.Level
}

type LogrusHook struct {
	Hostname string
	Context  string
	Type     string
}

func (hook *LogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook *LogrusHook) Fire(entry *logrus.Entry) error {
	entry.Data["host"] = hook.Hostname
	entry.Data["context"] = hook.Context
	entry.Data["type"] = hook.Type
	return nil
}

func New(
	filename string,
	maxSizeMB int,
	maxAgeDays int,
	maxBackups int,
	compress bool,
	logToConsole bool,
	level logrus.Level,
	customLevels map[string]logrus.Level,
) (definitions.LoggerFactory, error) {
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
		Level:        level,
		CustomLevels: customLevels,
	}, nil
}

func (d *DefaultLoggerFactory) GetLogger(loggerType string, name string) *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.AddHook(&LogrusHook{
		Hostname: d.hostname,
		Context:  name,
		Type:     loggerType,
	})
	logger.SetLevel(d.Level)
	if level, exists := d.CustomLevels[loggerType]; exists {
		logger.SetLevel(level)
	}

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
