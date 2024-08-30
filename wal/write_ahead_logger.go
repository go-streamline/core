package wal

import (
	"bufio"
	"encoding/json"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

type DefaultWriteAheadLogger struct {
	logger   *logrus.Logger
	log      *logrus.Logger
	filePath string
	enabled  bool
}

func NewWriteAheadLogger(logFilePath string, conf config.WriteAheadLogging, log *logrus.Logger) (definitions.WriteAheadLogger, error) {
	err := utils.CreateDirsIfNotExist(logFilePath, "")
	if err != nil {
		return nil, err
	}
	walLogger := logrus.New()

	walLogger.Out = &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    conf.MaxSizeMB,
		MaxBackups: conf.MaxBackups,
		MaxAge:     conf.MaxAgeDays,
		Compress:   true,
	}

	walLogger.SetFormatter(&logrus.JSONFormatter{})
	walLogger.SetLevel(logrus.InfoLevel)

	return &DefaultWriteAheadLogger{
		logger:   walLogger,
		filePath: logFilePath,
		enabled:  conf.Enabled,
		log:      log,
	}, nil
}

func (l *DefaultWriteAheadLogger) ReadEntries() ([]definitions.LogEntry, error) {
	if !l.enabled {
		return nil, nil
	}
	l.log.Debugf("reading WAL entries from %s", l.filePath)

	if _, err := os.Stat(l.filePath); os.IsNotExist(err) {
		l.log.Debugf("WAL file %s does not exist", l.filePath)
		return nil, nil
	}

	file, err := os.Open(l.filePath)
	if err != nil {
		return nil, err
	}
	l.log.Debugf("opened WAL file %s", l.filePath)
	defer file.Close()

	var entries []definitions.LogEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var entry definitions.LogEntry
		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	l.log.Debugf("read %d WAL entries", len(entries))

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	l.log.Debugf("finished reading WAL entries")

	return entries, nil
}

func (l *DefaultWriteAheadLogger) WriteEntry(entry definitions.LogEntry) {
	if !l.enabled {
		return
	}
	l.logger.WithFields(logrus.Fields{
		"session_id":     entry.SessionID.String(),
		"processor_name": entry.ProcessorName,
		"processor_id":   entry.ProcessorID,
		"flow_id":        entry.FlowID.String(),
		"input_file":     entry.InputFile,
		"output_file":    entry.OutputFile,
		"flow_object":    entry.FlowObject,
		"retry_count":    entry.RetryCount,
	}).Info("WAL entry recorded")
}
