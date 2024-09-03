package persist

import (
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type triggerProcessorModel struct {
	ID            uuid.UUID              `gorm:"type:uuid;primaryKey"`
	FlowID        uuid.UUID              `gorm:"column:flow_id;type:uuid;not null;index"`
	Name          string                 `gorm:"type:varchar(255);not null"`               // Name of the trigger processor
	Type          string                 `gorm:"type:varchar(255);not null"`               // Type of the trigger processor
	LogLevel      logrus.Level           `gorm:"column:log_level;not null;default:'info'"` // Logging level for this trigger processor
	Configuration map[string]interface{} `gorm:"column:configuration;type:jsonb;not null"` // Configuration for the trigger processor
	ScheduleType  int                    `gorm:"column:schedule_type;not null;default:0"`  // 0 for EventDriven, 1 for CronDriven
	CronExpr      string                 `gorm:"type:varchar(255)"`                        // Cron expression for CronDriven trigger processors
}

// BeforeCreate hook to generate a UUID if not provided.
func (t *triggerProcessorModel) BeforeCreate(tx *gorm.DB) (err error) {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	return
}

// TableName specifies the table name for GORM.
func (t *triggerProcessorModel) TableName() string {
	return "trigger_processors"
}
