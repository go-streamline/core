package persist

import (
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	ErrReferenceProcessorNotFound       = fmt.Errorf("reference processor not found")
	ErrCouldNotRunMigrations            = fmt.Errorf("could not run migrations")
	ErrFailedToGetTotalCount            = fmt.Errorf("failed to get total count")
	ErrFailedToUpdateFlowOrder          = fmt.Errorf("failed to update flow order")
	ErrFailedToCreateProcessor          = fmt.Errorf("failed to create processor")
	ErrFailedToUpdateReferenceProcessor = fmt.Errorf("failed to update reference processor")
)

type DBFlowManager struct {
	db *gorm.DB
}

func NewDefaultFlowManager(db *gorm.DB) (definitions.FlowManager, error) {
	err := db.AutoMigrate(&flowModel{}, &processorModel{})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotRunMigrations, err)
	}
	return &DBFlowManager{db: db}, nil
}

func (fm *DBFlowManager) ListFlows(pagination *definitions.PaginationRequest) (definitions.PaginatedData[*definitions.Flow], error) {
	var flowsModels []*flowModel
	var totalCount int64

	query := fm.db.Model(&flowModel{}).Count(&totalCount)
	if query.Error != nil {
		return definitions.PaginatedData[*definitions.Flow]{}, fmt.Errorf("%w: %v", ErrFailedToGetTotalCount, query.Error)
	}
	query = query.Offset(pagination.Offset()).Limit(pagination.Limit()).Find(&flowsModels)

	if query.Error != nil {
		return definitions.PaginatedData[*definitions.Flow]{}, query.Error
	}

	flows := make([]*definitions.Flow, len(flowsModels))
	for i, flow := range flowsModels {
		flows[i] = fm.convertFlowModelToFlow(flow)
	}

	return definitions.PaginatedData[*definitions.Flow]{
		Data:       flows,
		TotalCount: int(totalCount),
	}, nil
}

func (fm *DBFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
	var flow flowModel
	err := fm.db.First(&flow, "id = ?", flowID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertFlowModelToFlow(&flow), nil
}

func (fm *DBFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	var processors []processorModel
	err := fm.db.Where("flow_id = ? AND previous_processor_id IS NULL", flowID).
		Order("flow_order asc").Find(&processors).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorsToSimpleProcessors(processors), nil
}

func (fm *DBFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) (*definitions.SimpleProcessor, error) {
	var processor processorModel
	err := fm.db.Where("flow_id = ? AND next_processor_id IS NULL", flowID).
		Order("flow_order desc").First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorModelToSimpleProcessor(&processor), nil
}

func (fm *DBFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*definitions.SimpleProcessor, error) {
	var processor processorModel
	err := fm.db.Where("flow_id = ? AND id = ?", flowID, processorID).First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorModelToSimpleProcessor(&processor), nil
}

func (fm *DBFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	var nextProcessors []processorModel
	err := fm.db.Where("flow_id = ? AND previous_processor_id = ?", flowID, processorID).
		Order("flow_order asc").
		Find(&nextProcessors).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorsToSimpleProcessors(nextProcessors), nil
}

func (fm *DBFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	return fm.db.Transaction(func(tx *gorm.DB) error {
		referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
		if err != nil {
			return err
		}
		if referenceProcessor == nil {
			return ErrReferenceProcessorNotFound
		}

		err = tx.Model(&processorModel{}).
			Where("flow_id = ? AND flow_order >= ?", flowID, referenceProcessor.FlowOrder).
			Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateFlowOrder, err)
		}
		processor.FlowOrder = referenceProcessor.FlowOrder

		modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
		modelProcessor.FlowID = flowID
		err = tx.Create(modelProcessor).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
		}

		err = tx.Model(&processorModel{}).
			Where("id = ?", modelProcessor.ID).
			Update("next_processor_id", referenceProcessorID).Error

		return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
	})
}

func (fm *DBFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	return fm.db.Transaction(func(tx *gorm.DB) error {
		referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
		if err != nil {
			return err
		}
		if referenceProcessor == nil {
			return ErrReferenceProcessorNotFound
		}

		processor.FlowOrder = referenceProcessor.FlowOrder + 1
		modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
		err = tx.Model(&processorModel{}).
			Where("flow_id = ? AND flow_order > ?", flowID, referenceProcessor.FlowOrder).
			Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateFlowOrder, err)
		}

		modelProcessor.FlowID = flowID
		err = tx.Create(modelProcessor).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
		}

		err = tx.Model(&processorModel{}).
			Where("flow_id = ? AND id = ?", flowID, referenceProcessorID).
			Update("next_processor_id", modelProcessor.ID).Error

		return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
	})
}

func (fm *DBFlowManager) SaveFlow(flow *definitions.Flow) error {
	modelFlow := fm.convertFlowToFlowModel(flow)
	return fm.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(&modelFlow).Error; err != nil {
			return err
		}
		for _, processor := range flow.Processors {
			modelProcessor := fm.convertSimpleProcessorToProcessorModel(&processor)
			modelProcessor.FlowID = modelFlow.ID
			if err := tx.Save(modelProcessor).Error; err != nil {
				return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
			}
		}
		return nil
	})
}

func (fm *DBFlowManager) convertFlowModelsToFlows(flows []flowModel) []definitions.Flow {
	interfaceFlows := make([]definitions.Flow, len(flows))
	for i, flow := range flows {
		interfaceFlows[i] = *fm.convertFlowModelToFlow(&flow)
	}
	return interfaceFlows
}

func (fm *DBFlowManager) convertProcessorsToSimpleProcessors(processors []processorModel) []definitions.SimpleProcessor {
	simpleProcessors := make([]definitions.SimpleProcessor, len(processors))
	for i, processor := range processors {
		simpleProcessors[i] = *fm.convertProcessorModelToSimpleProcessor(&processor)
	}
	return simpleProcessors
}

func (fm *DBFlowManager) convertProcessorModelToSimpleProcessor(processor *processorModel) *definitions.SimpleProcessor {
	return &definitions.SimpleProcessor{
		ID:         processor.ID,
		FlowID:     processor.FlowID,
		Name:       processor.Name,
		Type:       processor.Type,
		FlowOrder:  processor.FlowOrder,
		Config:     processor.Configuration,
		MaxRetries: processor.MaxRetries,
		LogLevel:   processor.LogLevel,
	}
}

func (fm *DBFlowManager) convertSimpleProcessorToProcessorModel(processor *definitions.SimpleProcessor) *processorModel {
	return &processorModel{
		ID:            processor.ID,
		FlowID:        processor.FlowID,
		Name:          processor.Name,
		Type:          processor.Type,
		FlowOrder:     processor.FlowOrder,
		Configuration: processor.Config,
		MaxRetries:    processor.MaxRetries,
		LogLevel:      processor.LogLevel,
	}
}

func (fm *DBFlowManager) convertFlowModelToFlow(flow *flowModel) *definitions.Flow {
	return &definitions.Flow{
		ID:          flow.ID,
		Name:        flow.Name,
		Description: flow.Description,
		Processors:  fm.convertProcessorsToSimpleProcessors(flow.Processors),
	}
}

func (fm *DBFlowManager) convertFlowToFlowModel(flow *definitions.Flow) *flowModel {
	modelFlow := &flowModel{
		ID:          flow.ID,
		Name:        flow.Name,
		Description: flow.Description,
		Processors:  make([]processorModel, len(flow.Processors)),
	}
	for i, processor := range flow.Processors {
		modelFlow.Processors[i] = *fm.convertSimpleProcessorToProcessorModel(&processor)
	}
	return modelFlow
}
