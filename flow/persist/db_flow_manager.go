package persist

import (
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/lib/v4/cache"
	ristrettostore "github.com/eko/gocache/store/ristretto/v4"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"time"
)

var (
	ErrFailedToInitializeCache          = fmt.Errorf("failed to initialize cache")
	ErrReferenceProcessorNotFound       = fmt.Errorf("reference processor not found")
	ErrCouldNotRunMigrations            = fmt.Errorf("could not run migrations")
	ErrFailedToGetTotalCount            = fmt.Errorf("failed to get total count")
	ErrFailedToCreateProcessor          = fmt.Errorf("failed to create processor")
	ErrFailedToUpdateReferenceProcessor = fmt.Errorf("failed to update reference processor")
	ErrProcessorNotFound                = fmt.Errorf("processor not found")
)

type DBFlowManager struct {
	db        *gorm.DB
	flowCache *cache.Cache[*definitions.Flow]
	ctx       context.Context
}

// NewDBFlowManager creates a new instance of DBFlowManager with an in-memory cache for flows.
func NewDBFlowManager(db *gorm.DB) (definitions.FlowManager, error) {
	err := db.AutoMigrate(&flowModel{}, &processorModel{}, &triggerProcessorModel{})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotRunMigrations, err)
	}

	ristrettoCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     100,
		BufferItems: 64,
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrFailedToInitializeCache, err)
	}

	ristrettoStore := ristrettostore.NewRistretto(ristrettoCache)
	flowCache := cache.New[*definitions.Flow](ristrettoStore)

	return &DBFlowManager{
		db:        db,
		flowCache: flowCache,
		ctx:       context.Background(),
	}, nil
}

func (fm *DBFlowManager) GetFlowProcessors(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	var processorModels []processorModel
	query := fm.db.Where("flow_id = ?", flowID).Find(&processorModels)
	if query.Error != nil {
		if errors.Is(query.Error, gorm.ErrRecordNotFound) {
			return []definitions.SimpleProcessor{}, nil
		}
		return nil, query.Error
	}

	return fm.convertProcessorsToSimpleProcessors(processorModels), nil
}

// ListFlows lists all flows with pagination and filters based on the last update time.
func (fm *DBFlowManager) ListFlows(pagination *definitions.PaginationRequest, since time.Time) (definitions.PaginatedData[*definitions.Flow], error) {
	var flowModels []*flowModel
	var totalCount int64

	query := fm.db.Model(&flowModel{}).Where("updated_at > ?", since).Count(&totalCount)
	if query.Error != nil {
		return definitions.PaginatedData[*definitions.Flow]{}, fmt.Errorf("%w: %v", ErrFailedToGetTotalCount, query.Error)
	}
	query = query.Offset(pagination.Offset()).Limit(pagination.Limit()).Find(&flowModels)

	if query.Error != nil {
		return definitions.PaginatedData[*definitions.Flow]{}, query.Error
	}

	flows := make([]*definitions.Flow, len(flowModels))
	for i, flow := range flowModels {
		flows[i] = fm.convertFlowModelToFlow(flow)
	}

	return definitions.PaginatedData[*definitions.Flow]{
		Data:       flows,
		TotalCount: int(totalCount),
	}, nil
}

// GetFlowByID retrieves a flow by its ID, using cache if available.
func (fm *DBFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
	cachedFlow, err := fm.flowCache.Get(fm.ctx, flowID.String())
	if err == nil {
		logrus.WithError(err).Warn("error getting flow from cache")
	}
	if cachedFlow != nil {
		return cachedFlow, nil
	}

	var flow flowModel
	err = fm.db.Preload("Processors").Preload("TriggerProcessors").First(&flow, "id = ?", flowID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	result := fm.convertFlowModelToFlow(&flow)
	_ = fm.flowCache.Set(fm.ctx, flowID.String(), result)

	return result, nil
}

// GetFirstProcessorsForFlow retrieves the first processors in a flow.
func (fm *DBFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	return flow.Processors, nil
}

// GetTriggerProcessorsForFlow retrieves the trigger processors for a given flow.
func (fm *DBFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	var triggerProcessorModels []*triggerProcessorModel
	err := fm.db.Where("flow_id = ?", flowID).Find(&triggerProcessorModels).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrProcessorNotFound
		}
		return nil, err
	}

	triggerProcessors := make([]*definitions.SimpleTriggerProcessor, len(triggerProcessorModels))
	for i, model := range triggerProcessorModels {
		triggerProcessors[i] = fm.convertTriggerProcessorModelToSimpleTriggerProcessor(model)
	}
	return triggerProcessors, nil
}

func (fm *DBFlowManager) GetProcessors(ids []uuid.UUID) ([]definitions.SimpleProcessor, error) {
	var models []processorModel
	err := fm.db.Find(&models, "id IN ?", ids).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrProcessorNotFound
		}
		return nil, err
	}

	return fm.convertProcessorsToSimpleProcessors(models), nil
}

// AddProcessorToFlowBefore adds a processor to the flow before a reference processor.
// todo: change it so a first processor would move to be next
func (fm *DBFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	return fm.db.Transaction(func(tx *gorm.DB) error {
		referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
		if err != nil {
			return err
		}
		if referenceProcessor == nil {
			return ErrReferenceProcessorNotFound
		}

		// Set this processor's next to the reference processor
		processor.NextProcessorIDs = []uuid.UUID{referenceProcessorID}

		modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
		modelProcessor.FlowID = flowID

		err = tx.Create(modelProcessor).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
		}

		err = tx.Save(fm.convertSimpleProcessorToProcessorModel(referenceProcessor)).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
		}

		_ = fm.flowCache.Delete(fm.ctx, flowID.String())

		return nil
	})
}

// AddProcessorToFlowAfter adds a processor to the flow after a reference processor.
func (fm *DBFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	return fm.db.Transaction(func(tx *gorm.DB) error {
		referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
		if err != nil {
			return err
		}
		if referenceProcessor == nil {
			return ErrReferenceProcessorNotFound
		}

		// Set the reference processor's next to this processor
		referenceProcessor.NextProcessorIDs = append(referenceProcessor.NextProcessorIDs, processor.ID)

		modelProcessor := fm.convertSimpleProcessorToProcessorModel(processor)
		modelProcessor.FlowID = flowID

		err = tx.Create(modelProcessor).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
		}

		err = tx.Save(fm.convertSimpleProcessorToProcessorModel(referenceProcessor)).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
		}

		_ = fm.flowCache.Delete(fm.ctx, flowID.String())

		return nil
	})
}

// SaveFlow saves the flow and its processors to the database.
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

		for _, triggerProcessor := range flow.TriggerProcessors {
			modelTriggerProcessor := fm.convertSimpleTriggerProcessorToTriggerProcessorModel(&triggerProcessor)
			modelTriggerProcessor.FlowID = modelFlow.ID
			if err := tx.Save(modelTriggerProcessor).Error; err != nil {
				return fmt.Errorf("%w: %v", ErrFailedToCreateProcessor, err)
			}
		}

		// Invalidate cache for the flow
		_ = fm.flowCache.Delete(fm.ctx, flow.ID.String())

		return nil
	})
}

// GetProcessorByID retrieves a processor by its ID within a flow.
func (fm *DBFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	for _, processor := range flow.Processors {
		if processor.ID == processorID {
			return &processor, nil
		}
	}
	return nil, nil
}

// GetLastProcessorForFlow retrieves the last processor(s) for a flow.
// This function identifies processors that do not have any next processors.
func (fm *DBFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	var lastProcessors []definitions.SimpleProcessor
	for _, processor := range flow.Processors {
		// Last processors are those without next processors
		if len(processor.NextProcessorIDs) == 0 {
			lastProcessors = append(lastProcessors, processor)
		}
	}
	return lastProcessors, nil
}

// GetLastUpdateTime retrieves the last update time for the specified flow IDs.
func (fm *DBFlowManager) GetLastUpdateTime(flowIDs []uuid.UUID) (map[uuid.UUID]time.Time, error) {
	var flowModels []flowModel
	err := fm.db.Find(&flowModels, "id IN ?", flowIDs).Error
	if err != nil {
		return nil, err
	}

	lastUpdateTimes := make(map[uuid.UUID]time.Time)
	for _, flow := range flowModels {
		lastUpdateTimes[flow.ID] = flow.UpdatedAt
	}
	return lastUpdateTimes, nil
}

// SetFlowActive sets the active state of a flow.
func (fm *DBFlowManager) SetFlowActive(flowID uuid.UUID, active bool) error {
	return fm.db.Model(&flowModel{}).Where("id = ?", flowID).Update("active", active).Error
}

// Convert functions

func (fm *DBFlowManager) convertFlowModelsToFlows(flowModels []flowModel) []definitions.Flow {
	flows := make([]definitions.Flow, len(flowModels))
	for i, flow := range flowModels {
		flows[i] = *fm.convertFlowModelToFlow(&flow)
	}
	return flows
}

func (fm *DBFlowManager) convertProcessorsToSimpleProcessors(processorModels []processorModel) []definitions.SimpleProcessor {
	simpleProcessors := make([]definitions.SimpleProcessor, len(processorModels))
	for i, processor := range processorModels {
		simpleProcessors[i] = *fm.convertProcessorModelToSimpleProcessor(&processor)
	}
	return simpleProcessors
}

func (fm *DBFlowManager) convertTriggerProcessorsToSimpleTriggerProcessors(triggerProcessorModels []triggerProcessorModel) []definitions.SimpleTriggerProcessor {
	simpleTriggerProcessors := make([]definitions.SimpleTriggerProcessor, len(triggerProcessorModels))
	for i, triggerProcessor := range triggerProcessorModels {
		simpleTriggerProcessors[i] = *fm.convertTriggerProcessorModelToSimpleTriggerProcessor(&triggerProcessor)
	}
	return simpleTriggerProcessors
}

func (fm *DBFlowManager) convertProcessorModelToSimpleProcessor(processor *processorModel) *definitions.SimpleProcessor {
	return &definitions.SimpleProcessor{
		ID:               processor.ID,
		FlowID:           processor.FlowID,
		Name:             processor.Name,
		Type:             processor.Type,
		Config:           processor.Configuration,
		MaxRetries:       processor.MaxRetries,
		LogLevel:         processor.LogLevel,
		Enabled:          processor.Enabled,
		NextProcessorIDs: processor.NextProcessorIDs, // Use the new field for next processors
	}
}

func (fm *DBFlowManager) convertTriggerProcessorModelToSimpleTriggerProcessor(model *triggerProcessorModel) *definitions.SimpleTriggerProcessor {
	return &definitions.SimpleTriggerProcessor{
		ID:           model.ID,
		FlowID:       model.FlowID,
		Name:         model.Name,
		Type:         model.Type,
		ScheduleType: definitions.ScheduleType(model.ScheduleType),
		CronExpr:     model.CronExpr,
		Config:       model.Configuration,
		LogLevel:     model.LogLevel,
		Enabled:      model.Enabled,
		SingleNode:   model.SingleNode,
	}
}

func (fm *DBFlowManager) convertSimpleProcessorToProcessorModel(processor *definitions.SimpleProcessor) *processorModel {
	return &processorModel{
		ID:               processor.ID,
		FlowID:           processor.FlowID,
		Name:             processor.Name,
		Type:             processor.Type,
		Configuration:    processor.Config,
		MaxRetries:       processor.MaxRetries,
		LogLevel:         processor.LogLevel,
		Enabled:          processor.Enabled,
		NextProcessorIDs: processor.NextProcessorIDs, // Use the new field for next processors
	}
}

func (fm *DBFlowManager) convertSimpleTriggerProcessorToTriggerProcessorModel(triggerProcessor *definitions.SimpleTriggerProcessor) *triggerProcessorModel {
	return &triggerProcessorModel{
		ID:            triggerProcessor.ID,
		FlowID:        triggerProcessor.FlowID,
		Name:          triggerProcessor.Name,
		Type:          triggerProcessor.Type,
		ScheduleType:  int(triggerProcessor.ScheduleType),
		CronExpr:      triggerProcessor.CronExpr,
		Configuration: triggerProcessor.Config,
		LogLevel:      triggerProcessor.LogLevel,
		SingleNode:    triggerProcessor.SingleNode,
		Enabled:       triggerProcessor.Enabled,
	}
}

func (fm *DBFlowManager) convertFlowModelToFlow(flow *flowModel) *definitions.Flow {
	return &definitions.Flow{
		ID:                flow.ID,
		Name:              flow.Name,
		Description:       flow.Description,
		Processors:        fm.convertProcessorsToSimpleProcessors(flow.Processors),
		TriggerProcessors: fm.convertTriggerProcessorsToSimpleTriggerProcessors(flow.TriggerProcessors),
		Active:            flow.Active,
	}
}

func (fm *DBFlowManager) convertFlowToFlowModel(flow *definitions.Flow) *flowModel {
	modelFlow := &flowModel{
		ID:          flow.ID,
		Name:        flow.Name,
		Description: flow.Description,
		Active:      flow.Active,
		Processors:  make([]processorModel, len(flow.Processors)),
	}
	for i, processor := range flow.Processors {
		modelFlow.Processors[i] = *fm.convertSimpleProcessorToProcessorModel(&processor)
	}
	return modelFlow
}
