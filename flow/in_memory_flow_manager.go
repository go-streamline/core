package flow

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"sync"
)

var (
	ErrFlowNotFound          = fmt.Errorf("flow not found")
	ErrProcessorNotFound     = fmt.Errorf("processor not found")
	ErrLastProcessorNotFound = fmt.Errorf("last processor not found")
)

type InMemoryFlowManager struct {
	flows      map[uuid.UUID]*definitions.Flow
	processors map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor
	mu         sync.RWMutex
}

func NewInMemoryFlowManager() definitions.FlowManager {
	return &InMemoryFlowManager{
		flows:      make(map[uuid.UUID]*definitions.Flow),
		processors: make(map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor),
	}
}

func (fm *InMemoryFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, fmt.Errorf("failed to get first processors: %w", ErrFlowNotFound)
	}

	var firstProcessors []definitions.SimpleProcessor
	for _, processor := range fm.processors[flowID] {
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
	for _, processor := range fm.processors[flowID] {
		if lastProcessor == nil || processor.FlowOrder > lastProcessor.FlowOrder {
			lastProcessor = processor
		}
	}
	if lastProcessor == nil {
		return nil, ErrLastProcessorNotFound
	}
	return lastProcessor, nil
}

func (fm *InMemoryFlowManager) ListFlows(pagination *definitions.PaginationRequest) (definitions.PaginatedData[*definitions.Flow], error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flows := make([]*definitions.Flow, 0, len(fm.flows))
	for _, flow := range fm.flows {
		flows = append(flows, flow)
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

	processors, exists := fm.processors[flowID]
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

	processors, exists := fm.processors[flowID]
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

	for _, proc := range fm.processors[flowID] {
		if proc.FlowOrder >= referenceProcessor.FlowOrder {
			proc.FlowOrder++
		}
	}

	if fm.processors[flowID] == nil {
		fm.processors[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}

	fm.processors[flowID][processor.ID] = processor
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

	for _, proc := range fm.processors[flowID] {
		if proc.FlowOrder > referenceProcessor.FlowOrder {
			proc.FlowOrder++
		}
	}

	if fm.processors[flowID] == nil {
		fm.processors[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}

	fm.processors[flowID][processor.ID] = processor
	return nil
}

func (fm *InMemoryFlowManager) SaveFlow(flow *definitions.Flow) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.flows[flow.ID] = flow
	if fm.processors[flow.ID] == nil {
		fm.processors[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	for _, processor := range flow.Processors {
		fm.processors[flow.ID][processor.ID] = &processor
	}
	return nil
}
