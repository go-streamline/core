package wal

import (
	"bufio"
	"encoding/json"
	"github.com/cespare/xxhash/v2"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strconv"
)

type DefaultWriteAheadLogger struct {
	logger   *logrus.Logger
	log      *logrus.Logger
	filePath string
	enabled  bool
}

func NewWriteAheadLogger(
	walFilePath string,
	conf config.WriteAheadLogging,
	logFactory definitions.LoggerFactory,
) (definitions.WriteAheadLogger, error) {
	err := utils.CreateDirsIfNotExist(walFilePath)
	if err != nil {
		return nil, err
	}
	walLogger := logrus.New()

	walLogger.Out = &lumberjack.Logger{
		Filename:   walFilePath,
		MaxSize:    conf.MaxSizeMB,
		MaxBackups: conf.MaxBackups,
		MaxAge:     conf.MaxAgeDays,
		Compress:   true,
	}

	walLogger.SetFormatter(&logrus.JSONFormatter{})
	walLogger.SetLevel(logrus.InfoLevel)

	return &DefaultWriteAheadLogger{
		logger:   walLogger,
		filePath: walFilePath,
		enabled:  conf.Enabled,
		log:      logFactory.GetLogger("wal", strconv.FormatUint(xxhash.Sum64String(walFilePath), 10)),
	}, nil
}

// WriteEntry writes a log entry to the WAL with the option to mark it as complete
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
		"is_complete":    entry.IsComplete,
	}).Info("WAL entry recorded")
}

// ReadEntries reads all entries from the WAL file
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

// ReadLastEntries retrieves the last in-progress entries per session
func (l *DefaultWriteAheadLogger) ReadLastEntries() ([]definitions.LogEntry, error) {
	entries, err := l.ReadEntries()
	if err != nil {
		return nil, err
	}

	// track the last in-progress entry for each session
	lastEntries := make(map[uuid.UUID]definitions.LogEntry)

	for _, entry := range entries {
		// only consider non-complete entries
		if !entry.IsComplete {
			lastEntries[entry.SessionID] = entry
		}
	}

	// collect the last active entries
	var result []definitions.LogEntry
	for _, entry := range lastEntries {
		result = append(result, entry)
	}

	return result, nil
}
