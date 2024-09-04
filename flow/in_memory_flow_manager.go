package flow

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"sync"
	"time"
)

var (
	ErrFlowNotFound          = fmt.Errorf("flow not found")
	ErrProcessorNotFound     = fmt.Errorf("processor not found")
	ErrLastProcessorNotFound = fmt.Errorf("last processor not found")
)

type InMemoryFlowManager struct {
	flows                  map[uuid.UUID]*definitions.Flow
	flowToProcessor        map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor
	flowToTriggerProcessor map[uuid.UUID]map[uuid.UUID]*definitions.SimpleTriggerProcessor
	flowUpdates            map[uuid.UUID]time.Time
	mu                     sync.RWMutex
}

func NewInMemoryFlowManager() definitions.FlowManager {
	return &InMemoryFlowManager{
		flows:                  make(map[uuid.UUID]*definitions.Flow),
		flowToProcessor:        make(map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor),
		flowToTriggerProcessor: make(map[uuid.UUID]map[uuid.UUID]*definitions.SimpleTriggerProcessor),
		flowUpdates:            make(map[uuid.UUID]time.Time),
	}
}

func (fm *InMemoryFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, fmt.Errorf("failed to get first flowToProcessor: %w", ErrFlowNotFound)
	}

	var firstProcessors []definitions.SimpleProcessor
	for _, processor := range fm.flowToProcessor[flowID] {
		if processor.FlowOrder == 0 {
			firstProcessors = append(firstProcessors, *processor)
		}
	}
	return firstProcessors, nil
}

func (fm *InMemoryFlowManager) GetLastProcessorForFlow(flowID uuid.UUID) (*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, ErrFlowNotFound
	}

	var lastProcessor *definitions.SimpleProcessor
	for _, processor := range fm.flowToProcessor[flowID] {
		if lastProcessor == nil || processor.FlowOrder > lastProcessor.FlowOrder {
			lastProcessor = processor
		}
	}
	if lastProcessor == nil {
		return nil, ErrLastProcessorNotFound
	}
	return lastProcessor, nil
}

func (fm *InMemoryFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, fmt.Errorf("failed to get trigger flowToProcessor: %w", ErrFlowNotFound)
	}

	triggerProcessors := make([]*definitions.SimpleTriggerProcessor, 0)
	for _, triggerProcessor := range fm.flowToTriggerProcessor[flowID] {
		triggerProcessors = append(triggerProcessors, triggerProcessor)
	}
	return triggerProcessors, nil
}

func (fm *InMemoryFlowManager) ListFlows(pagination *definitions.PaginationRequest, since time.Time) (definitions.PaginatedData[*definitions.Flow], error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flows := make([]*definitions.Flow, 0)
	for _, flow := range fm.flows {
		if update, exists := fm.flowUpdates[flow.ID]; exists && update.After(since) {
			flows = append(flows, flow)
		}
	}

	totalCount := len(flows)
	start := pagination.Offset()
	end := start + pagination.Limit()

	if start > totalCount {
		return definitions.PaginatedData[*definitions.Flow]{
			Data:       []*definitions.Flow{},
			TotalCount: totalCount,
		}, nil
	}

	if end > totalCount {
		end = totalCount
	}

	return definitions.PaginatedData[*definitions.Flow]{
		Data:       flows[start:end],
		TotalCount: totalCount,
	}, nil
}

func (fm *InMemoryFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}
	return flow, nil
}

func (fm *InMemoryFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	processors, exists := fm.flowToProcessor[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	processor, exists := processors[processorID]
	if !exists {
		return nil, ErrProcessorNotFound
	}
	return processor, nil
}

func (fm *InMemoryFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	processors, exists := fm.flowToProcessor[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	processor, exists := processors[processorID]
	if !exists {
		return nil, ErrProcessorNotFound
	}

	var nextProcessors []definitions.SimpleProcessor
	for _, proc := range processors {
		if proc.FlowOrder > processor.FlowOrder {
			nextProcessors = append(nextProcessors, *proc)
		}
	}
	return nextProcessors, nil
}

func (fm *InMemoryFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return ErrProcessorNotFound
	}

	processor.FlowOrder = referenceProcessor.FlowOrder

	for _, proc := range fm.flowToProcessor[flowID] {
		if proc.FlowOrder >= referenceProcessor.FlowOrder {
			proc.FlowOrder++
		}
	}

	if fm.flowToProcessor[flowID] == nil {
		fm.flowToProcessor[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}

	fm.flowToProcessor[flowID][processor.ID] = processor
	fm.flowUpdates[flowID] = time.Now()
	return nil
}

func (fm *InMemoryFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return ErrProcessorNotFound
	}

	processor.FlowOrder = referenceProcessor.FlowOrder + 1

	for _, proc := range fm.flowToProcessor[flowID] {
		if proc.FlowOrder > referenceProcessor.FlowOrder {
			proc.FlowOrder++
		}
	}

	if fm.flowToProcessor[flowID] == nil {
		fm.flowToProcessor[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}

	fm.flowToProcessor[flowID][processor.ID] = processor
	fm.flowUpdates[flowID] = time.Now()
	return nil
}

func (fm *InMemoryFlowManager) SaveFlow(flow *definitions.Flow) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.flows[flow.ID] = flow
	if fm.flowToProcessor[flow.ID] == nil {
		fm.flowToProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	for _, processor := range flow.Processors {
		fm.flowToProcessor[flow.ID][processor.ID] = &processor
	}
	if fm.flowToTriggerProcessor[flow.ID] == nil {
		fm.flowToTriggerProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleTriggerProcessor)
	}
	for _, triggerProcessor := range flow.TriggerProcessors {
		fm.flowToTriggerProcessor[flow.ID][triggerProcessor.ID] = &triggerProcessor
	}
	fm.flowUpdates[flow.ID] = time.Now()
	return nil
}

func (fm *InMemoryFlowManager) GetLastUpdateTime(flowIDs []uuid.UUID) (map[uuid.UUID]time.Time, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	lastUpdates := make(map[uuid.UUID]time.Time)
	for _, flowID := range flowIDs {
		if update, exists := fm.flowUpdates[flowID]; exists {
			lastUpdates[flowID] = update
		} else {
			return nil, ErrFlowNotFound
		}
	}
	return lastUpdates, nil
}

func (fm *InMemoryFlowManager) SetFlowActive(flowID uuid.UUID, active bool) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return ErrFlowNotFound
	}

	flow.Active = active
	fm.flowUpdates[flowID] = time.Now()
	return nil
}
