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

type DBWriteAheadLogger struct {
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
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

func NewDBWriteAheadLogger(db *gorm.DB, log *logrus.Logger) (definitions.WriteAheadLogger, error) {
	if err := db.AutoMigrate(&walLogEntry{}); err != nil {
		return nil, err
	}

	return &DBWriteAheadLogger{
		db:  db,
		log: log,
	}, nil
}

func (l *DBWriteAheadLogger) ReadEntries() ([]definitions.LogEntry, error) {
	var walEntries []walLogEntry

	subQuery := l.db.Model(&walLogEntry{}).
		Select("session_id").
		Where("processor_name = ?", "__end__")

	// get all entries from incomplete sessions
	err := l.db.Where("session_id NOT IN (?)", subQuery).
		Order("created_at asc").Find(&walEntries).Error
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve WAL entries: %v", err)
	}

	// get the last entry for each session
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
		}
		logEntries = append(logEntries, logEntry)
	}

	l.log.Debugf("read %d WAL entries from DB", len(logEntries))
	return logEntries, nil
}

func (l *DBWriteAheadLogger) WriteEntry(entry definitions.LogEntry) {
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
	}).Info("WAL entry recorded in DB")
}
