package models

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Flow struct {
	ID          uuid.UUID `gorm:"type:uuid;primary_key;"`
	Name        string
	Description string
	Processors  []Processor `gorm:"foreignKey:FlowID"`
}

func (f *Flow) TableName() string {
	return "flows"
}

func (f *Flow) BeforeCreate(tx *gorm.DB) (err error) {
	f.ID = uuid.New()
	return
}
