package db

import (
	"encoding/json"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"time"
)

type WriteAheadLogger struct {
	db  *gorm.DB
	log *logrus.Logger
}

type walLogEntry struct {
	ID            uuid.UUID `gorm:"type:uuid;primary_key;"`
	SessionID     uuid.UUID `gorm:"type:uuid;index;not null"`
	ProcessorName string    `gorm:"type:varchar(255);not null"`
	ProcessorID   string    `gorm:"type:uuid;not null"`
	FlowID        uuid.UUID `gorm:"type:uuid;index;not null"`
	InputFile     string    `gorm:"type:text"`
	OutputFile    string    `gorm:"type:text"`
	FlowObject    []byte    `gorm:"type:jsonb"`
	RetryCount    int       `gorm:"not null"`
	IsComplete    bool      `gorm:"not null;default:false"` // indicates if the processor has completed
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

// NewDBWriteAheadLogger creates a new logger and ensures the table is migrated.
func NewDBWriteAheadLogger(db *gorm.DB, log *logrus.Logger) (definitions.WriteAheadLogger, error) {
	if err := db.AutoMigrate(&walLogEntry{}); err != nil {
		return nil, err
	}

	return &WriteAheadLogger{
		db:  db,
		log: log,
	}, nil
}

// ReadEntries reads all WAL entries from the database.
func (l *WriteAheadLogger) ReadEntries() ([]definitions.LogEntry, error) {
	var walEntries []walLogEntry

	err := l.db.Order("created_at asc").Find(&walEntries).Error
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve WAL entries: %v", err)
	}

	// Map database entries to LogEntry objects
	var logEntries []definitions.LogEntry
	for _, walEntry := range walEntries {
		var flowObject definitions.EngineFlowObject
		err := json.Unmarshal(walEntry.FlowObject, &flowObject)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal flow object: %v", err)
		}

		logEntry := definitions.LogEntry{
			SessionID:     walEntry.SessionID,
			ProcessorName: walEntry.ProcessorName,
			ProcessorID:   walEntry.ProcessorID,
			FlowID:        walEntry.FlowID,
			InputFile:     walEntry.InputFile,
			OutputFile:    walEntry.OutputFile,
			FlowObject:    flowObject,
			RetryCount:    walEntry.RetryCount,
			IsComplete:    walEntry.IsComplete, // map the completion flag
		}
		logEntries = append(logEntries, logEntry)
	}

	l.log.Debugf("read %d WAL entries from DB", len(logEntries))
	return logEntries, nil
}

// ReadLastEntries retrieves the last active entries (not completed) for each session.
func (l *WriteAheadLogger) ReadLastEntries() ([]definitions.LogEntry, error) {
	var walEntries []walLogEntry

	// Fetch only the last incomplete entries for each session
	subQuery := l.db.Model(&walLogEntry{}).
		Select("session_id, MAX(created_at) AS created_at").
		Where("is_complete = ?", false).
		Group("session_id")

	err := l.db.Joins("JOIN (?) AS sub ON wal_log_entries.session_id = sub.session_id AND wal_log_entries.created_at = sub.created_at", subQuery).
		Order("created_at asc").
		Find(&walEntries).Error
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve WAL entries: %v", err)
	}

	// Map to LogEntry
	var logEntries []definitions.LogEntry
	for _, walEntry := range walEntries {
		var flowObject definitions.EngineFlowObject
		err := json.Unmarshal(walEntry.FlowObject, &flowObject)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal flow object: %v", err)
		}

		logEntry := definitions.LogEntry{
			SessionID:     walEntry.SessionID,
			ProcessorName: walEntry.ProcessorName,
			ProcessorID:   walEntry.ProcessorID,
			FlowID:        walEntry.FlowID,
			InputFile:     walEntry.InputFile,
			OutputFile:    walEntry.OutputFile,
			FlowObject:    flowObject,
			RetryCount:    walEntry.RetryCount,
			IsComplete:    walEntry.IsComplete,
		}
		logEntries = append(logEntries, logEntry)
	}

	l.log.Debugf("read %d last active WAL entries from DB", len(logEntries))
	return logEntries, nil
}

// WriteEntry writes a log entry to the WAL table in the database.
func (l *WriteAheadLogger) WriteEntry(entry definitions.LogEntry) {
	flowObjectBytes, err := json.Marshal(entry.FlowObject)
	if err != nil {
		l.log.WithError(err).Error("failed to marshal FlowObject")
		return
	}

	walEntry := walLogEntry{
		ID:            uuid.New(),
		SessionID:     entry.SessionID,
		ProcessorName: entry.ProcessorName,
		ProcessorID:   entry.ProcessorID,
		FlowID:        entry.FlowID,
		InputFile:     entry.InputFile,
		OutputFile:    entry.OutputFile,
		FlowObject:    flowObjectBytes,
		RetryCount:    entry.RetryCount,
		IsComplete:    entry.IsComplete,
	}

	if err := l.db.Create(&walEntry).Error; err != nil {
		l.log.WithError(err).Error("failed to write WAL entry to DB")
		return
	}
	l.log.WithFields(logrus.Fields{
		"session_id":     walEntry.SessionID.String(),
		"processor_name": walEntry.ProcessorName,
		"processor_id":   walEntry.ProcessorID,
		"flow_id":        walEntry.FlowID.String(),
		"is_complete":    walEntry.IsComplete,
	}).Info("WAL entry recorded in DB")
}
