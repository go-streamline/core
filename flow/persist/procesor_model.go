package persist

import (
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type processorModel struct {
	ID                   uuid.UUID      `gorm:"type:uuid;primaryKey"`
	FlowID               uuid.UUID      `gorm:"column:flow_id;type:uuid;not null;index"`
	Name                 string         `gorm:"type:varchar(255);not null"`
	Type                 string         `gorm:"type:varchar(255);not null"`
	MaxRetries           int            `gorm:"column:max_retries;default:3"`
	Backoff              int            `gorm:"column:backoff;default:1"`
	LogLevel             logrus.Level   `gorm:"column:log_level;not null;default:'info'"`
	Configuration        map[string]any `gorm:"column:configuration;type:jsonb;not null"`
	NextProcessorIDs     []uuid.UUID    `gorm:"column:next_processor_ids;type:uuid[];index:idx_flow_next_processor"`
	PreviousProcessorIDs []uuid.UUID    `gorm:"column:previous_processor_ids;type:uuid[];index:idx_flow_previous_processor"`
	Enabled              bool           `gorm:"column:enabled;not null;default:true"`
}

func (p *processorModel) BeforeCreate(tx *gorm.DB) (err error) {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	return
}

func (p *processorModel) TableName() string {
	return "processors"
}
