package flow

import (
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/definitions/models"
	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"path"
)

var ErrCouldNotGetDBConnection = fmt.Errorf("could not get db connection")
var ErrCouldNotRunMigrations = fmt.Errorf("could not run migrations")

type DefaultFlowManager struct {
	db *gorm.DB
}

func NewDefaultFlowManager(db *gorm.DB, workDir string) (definitions.FlowManager, error) {
	db, err := runMigrations(db, workDir)
	if err != nil {
		return nil, err
	}
	return &DefaultFlowManager{db: db}, nil
}

func newSQLiteDB(workDir string) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(path.Join(workDir, "db", "go-streamline.db")), &gorm.Config{})
}

func runMigrations(db *gorm.DB, workDir string) (*gorm.DB, error) {
	var err error
	if db == nil {
		db, err = newSQLiteDB(workDir)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrCouldNotGetDBConnection, err)
		}
	}

	/*s, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotGetDBConnection, err)
	}

	if err = goose.Up(s, "./migrations"); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotRunMigrations, err)
	}*/ // todo: use another way for migrations

	return db, nil
}

func (fm *DefaultFlowManager) ListFlows() ([]models.Flow, error) {
	var flows []models.Flow
	err := fm.db.Find(&flows).Error
	if err != nil {
		return nil, err
	}
	return flows, nil
}

func (fm *DefaultFlowManager) GetFlowByID(flowID uuid.UUID) (*models.Flow, error) {
	var flow models.Flow
	err := fm.db.First(&flow, "id = ?", flowID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &flow, nil
}

func (fm *DefaultFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]models.Processor, error) {
	var processors []models.Processor
	err := fm.db.Where("flow_id = ? AND previous_processor_id IS NULL", flowID).
		Order("flow_order asc").Find(&processors).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return processors, nil
}

// GetLastProcessorForFlow returns the last processor executed in a given flow.
func (fm *DefaultFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) (*models.Processor, error) {
	var processor models.Processor
	err := fm.db.Where("flow_id = ? AND next_processor_id IS NULL", flowID).
		Order("flow_order desc").First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &processor, nil
}

// AddProcessorToFlow adds a processor to a flow. It can insert before or after a specified processor.
func (fm *DefaultFlowManager) AddProcessorToFlow(flowID uuid.UUID, processor *models.Processor, position string, referenceProcessorID uuid.UUID) error {
	var referenceProcessor models.Processor

	// Fetch the reference processor to determine the flow order
	err := fm.db.Where("flow_id = ? AND id = ?", flowID, referenceProcessorID).First(&referenceProcessor).Error
	if err != nil {
		return fmt.Errorf("reference processor not found: %w", err)
	}

	switch position {
	case "before":
		// Adjust flow orders to insert the new processor before the reference processor
		err = fm.db.Model(&models.Processor{}).
			Where("flow_id = ? AND flow_order >= ?", flowID, referenceProcessor.FlowOrder).
			Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
		if err != nil {
			return err
		}
		processor.FlowOrder = referenceProcessor.FlowOrder
	case "after":
		// Insert after the reference processor
		processor.FlowOrder = referenceProcessor.FlowOrder + 1
		// Adjust flow orders of subsequent processors
		err = fm.db.Model(&models.Processor{}).
			Where("flow_id = ? AND flow_order > ?", flowID, referenceProcessor.FlowOrder).
			Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid position: %s", position)
	}

	processor.FlowID = flowID
	err = fm.db.Create(processor).Error
	if err != nil {
		return err
	}

	// Link the new processor to its previous or next processor
	if position == "after" {
		err = fm.db.Model(&models.Processor{}).
			Where("id = ?", referenceProcessorID).
			Update("next_processor_id", processor.ID).Error
	} else {
		err = fm.db.Model(&models.Processor{}).
			Where("id = ?", processor.ID).
			Update("next_processor_id", referenceProcessorID).Error
	}

	return err
}

func (fm *DefaultFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*models.Processor, error) {
	var processor models.Processor
	err := fm.db.Where("flow_id = ? AND id = ?", flowID, processorID).First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &processor, nil
}

func (fm *DefaultFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]models.Processor, error) {
	var nextProcessors []models.Processor
	err := fm.db.Where("flow_id = ? AND previous_processor_id = ?", flowID, processorID).
		Order("flow_order asc").
		Find(&nextProcessors).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return nextProcessors, nil
}

func (fm *DefaultFlowManager) ListProcessorsForFlow(flowID uuid.UUID) ([]models.Processor, error) {
	var processors []models.Processor
	err := fm.db.Where("flow_id = ?", flowID).Order("flow_order asc").Find(&processors).Error
	if err != nil {
		return nil, err
	}
	return processors, nil
}
