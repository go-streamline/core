package persist

import (
	"database/sql/driver"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"strings"
)

type UUIDArray []uuid.UUID

func (u *UUIDArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		return u.parseString(src)
	case []byte:
		return u.parseString(string(src))
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type UUIDArray", src)
	}
}

// parseString parses the uuid array format (e.g., "{uuid1,uuid2}")
func (u *UUIDArray) parseString(src string) error {
	src = strings.Trim(src, "{}") // remove the curly braces
	if src == "" {
		*u = UUIDArray{}
		return nil
	}

	parts := strings.Split(src, ",")
	uuids := make([]uuid.UUID, len(parts))
	for i, part := range parts {
		id, err := uuid.Parse(part)
		if err != nil {
			return err
		}
		uuids[i] = id
	}

	*u = uuids
	return nil
}

// Value implements the driver.Valuer interface for UUIDArray
func (u UUIDArray) Value() (driver.Value, error) {
	if len(u) == 0 {
		return "{}", nil
	}

	strUuids := make([]string, len(u))
	for i, id := range u {
		strUuids[i] = id.String()
	}
	return "{" + strings.Join(strUuids, ",") + "}", nil
}

type processorModel struct {
	ID               uuid.UUID      `gorm:"type:uuid;primaryKey"`
	FlowID           uuid.UUID      `gorm:"column:flow_id;type:uuid;not null;index"`
	Name             string         `gorm:"type:varchar(255);not null"`
	Type             string         `gorm:"type:varchar(255);not null"`
	MaxRetries       int            `gorm:"column:max_retries;default:3"`
	BackoffSeconds   int            `gorm:"column:backoff_seconds;default:1"`
	LogLevel         string         `gorm:"column:log_level;not null;default:'info'"`
	Configuration    datatypes.JSON `gorm:"column:configuration;not null"`
	NextProcessorIDs UUIDArray      `gorm:"column:next_processor_ids;type:uuid[];index:idx_flow_next_processor"`
	Enabled          bool           `gorm:"column:enabled;not null;default:true"`
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
