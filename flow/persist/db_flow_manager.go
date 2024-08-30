package persist

import (
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var ErrCouldNotGetDBConnection = fmt.Errorf("could not get db connection")
var ErrReferenceProcessorNotFound = fmt.Errorf("reference processor not found")
var ErrCouldNotRunMigrations = fmt.Errorf("could not run migrations")

type DefaultFlowManager struct {
	db *gorm.DB
}

func NewDefaultFlowManager(db *gorm.DB) definitions.FlowManager {
	return &DefaultFlowManager{db: db}
}

func (fm *DefaultFlowManager) ListFlows() ([]definitions.Flow, error) {
	var flows []flowModel
	err := fm.db.Find(&flows).Error
	if err != nil {
		return nil, err
	}
	return fm.convertFlowsToInterface(flows), nil
}

func (fm *DefaultFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
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

func (fm *DefaultFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	var processors []processorModel
	err := fm.db.Where("flow_id = ? AND previous_processor_id IS NULL", flowID).
		Order("flow_order asc").Find(&processors).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorsToInterface(processors), nil
}

func (fm *DefaultFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) (*definitions.SimpleProcessor, error) {
	var processor processorModel
	err := fm.db.Where("flow_id = ? AND next_processor_id IS NULL", flowID).
		Order("flow_order desc").First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorToInterface(&processor), nil
}

func (fm *DefaultFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*definitions.SimpleProcessor, error) {
	var processor processorModel
	err := fm.db.Where("flow_id = ? AND id = ?", flowID, processorID).First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return fm.convertProcessorToInterface(&processor), nil
}

func (fm *DefaultFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]definitions.SimpleProcessor, error) {
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
	return fm.convertProcessorsToInterface(nextProcessors), nil
}

func (fm *DefaultFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}
	if referenceProcessor == nil {
		return ErrReferenceProcessorNotFound
	}

	err = fm.db.Model(&processorModel{}).
		Where("flow_id = ? AND flow_order >= ?", flowID, referenceProcessor.FlowOrder).
		Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
	if err != nil {
		return err
	}
	processor.FlowOrder = referenceProcessor.FlowOrder

	modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
	modelProcessor.FlowID = flowID
	err = fm.db.Create(modelProcessor).Error
	if err != nil {
		return err
	}

	err = fm.db.Model(&processorModel{}).
		Where("id = ?", modelProcessor.ID).
		Update("next_processor_id", referenceProcessorID).Error

	return err
}

func (fm *DefaultFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}
	if referenceProcessor == nil {
		return ErrReferenceProcessorNotFound
	}

	processor.FlowOrder = referenceProcessor.FlowOrder + 1
	modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
	err = fm.db.Model(&processorModel{}).
		Where("flow_id = ? AND flow_order > ?", flowID, referenceProcessor.FlowOrder).
		Update("flow_order", gorm.Expr("flow_order + ?", 1)).Error
	if err != nil {
		return err
	}

	modelProcessor.FlowID = flowID
	err = fm.db.Create(modelProcessor).Error
	if err != nil {
		return err
	}

	err = fm.db.Model(&processorModel{}).
		Where("id = ?", referenceProcessorID).
		Update("next_processor_id", modelProcessor.ID).Error

	return err
}

func (fm *DefaultFlowManager) SaveFlow(flow *definitions.Flow) error {
	modelFlow := fm.convertFlowToFlowModel(flow)
	return fm.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(&modelFlow).Error; err != nil {
			return err
		}
		for _, processor := range flow.Processors {
			modelProcessor := fm.convertSimpleProcessorToProcessorModel(&processor)
			modelProcessor.FlowID = modelFlow.ID
			if err := tx.Save(modelProcessor).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (fm *DefaultFlowManager) convertFlowsToInterface(flows []flowModel) []definitions.Flow {
	interfaceFlows := make([]definitions.Flow, len(flows))
	for i, flow := range flows {
		interfaceFlows[i] = *fm.convertFlowModelToFlow(&flow)
	}
	return interfaceFlows
}

func (fm *DefaultFlowManager) convertProcessorsToInterface(processors []processorModel) []definitions.SimpleProcessor {
	simpleProcessors := make([]definitions.SimpleProcessor, len(processors))
	for i, processor := range processors {
		simpleProcessors[i] = *fm.convertProcessorToInterface(&processor)
	}
	return simpleProcessors
}

func (fm *DefaultFlowManager) convertProcessorToInterface(processor *processorModel) *definitions.SimpleProcessor {
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

func (fm *DefaultFlowManager) convertSimpleProcessorToProcessorModel(processor *definitions.SimpleProcessor) *processorModel {
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

func (fm *DefaultFlowManager) convertFlowModelToFlow(flow *flowModel) *definitions.Flow {
	return &definitions.Flow{
		ID:          flow.ID,
		Name:        flow.Name,
		Description: flow.Description,
		Processors:  fm.convertProcessorsToInterface(flow.Processors),
	}
}

func (fm *DefaultFlowManager) convertFlowToFlowModel(flow *definitions.Flow) *flowModel {
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
