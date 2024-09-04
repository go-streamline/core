package persist

import (
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/lib/v4/cache"
	ristretto_store "github.com/eko/gocache/store/ristretto/v4"
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
	ErrFailedToUpdateFlowOrder          = fmt.Errorf("failed to update flow order")
	ErrFailedToCreateProcessor          = fmt.Errorf("failed to create processor")
	ErrFailedToUpdateReferenceProcessor = fmt.Errorf("failed to update reference processor")
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

	ristrettoStore := ristretto_store.NewRistretto(ristrettoCache)
	flowCache := cache.New[*definitions.Flow](ristrettoStore)

	return &DBFlowManager{
		db:        db,
		flowCache: flowCache,
		ctx:       context.Background(), // Store a context for cache operations
	}, nil
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

	var firstProcessors []definitions.SimpleProcessor
	for _, processor := range flow.Processors {
		if processor.FlowOrder == 1 { // Assuming FlowOrder == 1 indicates the first processor
			firstProcessors = append(firstProcessors, processor)
		}
	}
	return firstProcessors, nil
}

// GetLastProcessorForFlow retrieves the last processor in a flow.
func (fm *DBFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) (*definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	var lastProcessor *definitions.SimpleProcessor
	maxOrder := 0
	for _, processor := range flow.Processors {
		if processor.FlowOrder > maxOrder {
			lastProcessor = &processor
			maxOrder = processor.FlowOrder
		}
	}
	return lastProcessor, nil
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

// GetNextProcessors retrieves the next processors in the flow after the given processor.
func (fm *DBFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	flow, err := fm.GetFlowByID(flowID)
	if err != nil || flow == nil {
		return nil, err
	}

	var nextProcessors []definitions.SimpleProcessor
	for _, processor := range flow.Processors {
		if processor.ID == processorID {
			nextOrder := processor.FlowOrder + 1
			for _, nextProcessor := range flow.Processors {
				if nextProcessor.FlowOrder == nextOrder {
					nextProcessors = append(nextProcessors, nextProcessor)
				}
			}
			break
		}
	}
	return nextProcessors, nil
}

// GetTriggerProcessorsForFlow retrieves the trigger processors for a given flow.
func (fm *DBFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	var triggerProcessorModels []*triggerProcessorModel
	err := fm.db.Where("flow_id = ?", flowID).Find(&triggerProcessorModels).Error
	if err != nil {
		return nil, err
	}

	triggerProcessors := make([]*definitions.SimpleTriggerProcessor, len(triggerProcessorModels))
	for i, model := range triggerProcessorModels {
		triggerProcessors[i] = fm.convertTriggerProcessorModelToSimpleTriggerProcessor(model)
	}
	return triggerProcessors, nil
}

// AddProcessorToFlowBefore adds a processor to the flow before a reference processor.
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
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
		}

		// Invalidate cache for the flow
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
			Where("flow_id = ? AND id = ?", referenceProcessorID).
			Update("next_processor_id", modelProcessor.ID).Error
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToUpdateReferenceProcessor, err)
		}

		// Invalidate cache for the flow
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
		ID:         processor.ID,
		FlowID:     processor.FlowID,
		Name:       processor.Name,
		Type:       processor.Type,
		FlowOrder:  processor.FlowOrder,
		Config:     processor.Configuration,
		MaxRetries: processor.MaxRetries,
		LogLevel:   processor.LogLevel,
		Enabled:    processor.Enabled,
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
		Enabled:       processor.Enabled,
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
		Processors:  make([]processorModel, len(flow.Processors)),
		Active:      flow.Active,
	}
	for i, processor := range flow.Processors {
		modelFlow.Processors[i] = *fm.convertSimpleProcessorToProcessorModel(&processor)
	}
	return modelFlow
}
