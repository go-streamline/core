package persist

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type triggerProcessorModel struct {
	ID            uuid.UUID      `gorm:"type:uuid;primaryKey"`
	FlowID        uuid.UUID      `gorm:"column:flow_id;type:uuid;not null;index"`
	Name          string         `gorm:"type:varchar(255);not null"`                // Name of the trigger processor
	Type          string         `gorm:"type:varchar(255);not null"`                // Type of the trigger processor
	LogLevel      string         `gorm:"column:log_level;not null;default:'info'"`  // Logging level for this trigger processor
	Configuration datatypes.JSON `gorm:"column:configuration;not null"`             // Configuration for the trigger processor
	CronExpr      string         `gorm:"type:varchar(255)"`                         // Cron expression for CronDriven trigger processors
	SingleNode    bool           `gorm:"column:single_node;not null;default:false"` // Indicates if the trigger processor should run on a single node
	Enabled       bool           `gorm:"column:enabled;not null;default:true"`      // Indicates if the trigger processor is enabled
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
