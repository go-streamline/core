package persist

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type flowModel struct {
	ID          uuid.UUID `gorm:"type:uuid;primary_key;"`
	Name        string
	Description string
	Processors  []processorModel `gorm:"foreignKey:FlowID"`
}

func (f *flowModel) TableName() string {
	return "flows"
}

func (f *flowModel) BeforeCreate(tx *gorm.DB) (err error) {
	f.ID = uuid.New()
	return
}
