package models

import (
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// Processor represents the processor configuration stored in the database.
type Processor struct {
	ID                  uuid.UUID      `gorm:"type:uuid;primaryKey"`
	FlowID              uuid.UUID      `gorm:"column:flow_id;type:uuid;not null"`
	Name                string         `gorm:"type:varchar(255);not null"`
	Type                string         `gorm:"type:varchar(255);not null"`
	FlowOrder           int            `gorm:"column:flow_order;not null"`
	MaxRetries          int            `gorm:"column:max_retries;default:3"`
	LogLevel            logrus.Level   `gorm:"column:log_level;not null;default:'info'"`
	Configuration       map[string]any `gorm:"column:configuration;type:jsonb;not null"`
	PreviousProcessorID uuid.UUID      `gorm:"column:previous_processor_id;type:uuid"`
	NextProcessorID     uuid.UUID      `gorm:"column:next_processor_id;type:uuid"`
}

func (p *Processor) TableName() string {
	return "processors"
}
