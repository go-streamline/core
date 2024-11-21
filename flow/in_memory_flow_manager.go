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

// GetFirstProcessorsForFlow retrieves processors that are not referenced as "next processors".
func (fm *InMemoryFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, ErrFlowNotFound
	}

	var firstProcessors []*definitions.SimpleProcessor
	// collect processors not listed in any NextProcessorIDs
	mentionedAsNext := make(map[uuid.UUID]bool)
	for _, processor := range fm.flowToProcessor[flowID] {
		for _, nextID := range processor.NextProcessorIDs {
			mentionedAsNext[nextID] = true
		}
	}

	for _, processor := range fm.flowToProcessor[flowID] {
		if !mentionedAsNext[processor.ID] {
			firstProcessors = append(firstProcessors, processor)
		}
	}

	return firstProcessors, nil
}

// GetFlowProcessors retrieves all processors for a given flow.
func (fm *InMemoryFlowManager) GetFlowProcessors(flowID uuid.UUID) ([]*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	processors, exists := fm.flowToProcessor[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	var allProcessors []*definitions.SimpleProcessor
	for _, processor := range processors {
		allProcessors = append(allProcessors, processor)
	}

	return allProcessors, nil
}

// GetProcessors retrieves processors by their unique identifiers.
func (fm *InMemoryFlowManager) GetProcessors(processorIDs []uuid.UUID) ([]*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	var result []*definitions.SimpleProcessor
	for _, id := range processorIDs {
		found := false
		for _, processors := range fm.flowToProcessor {
			if processor, exists := processors[id]; exists {
				result = append(result, processor)
				found = true
				break
			}
		}
		if !found {
			return nil, ErrProcessorNotFound
		}
	}

	return result, nil
}

// GetNextProcessors retrieves processors referenced in the NextProcessorIDs of the specified processor.
func (fm *InMemoryFlowManager) GetNextProcessors(flowID uuid.UUID, processorID uuid.UUID) ([]*definitions.SimpleProcessor, error) {
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

	var nextProcessors []*definitions.SimpleProcessor
	for _, nextID := range processor.NextProcessorIDs {
		if nextProcessor, ok := processors[nextID]; ok {
			nextProcessors = append(nextProcessors, nextProcessor)
		}
	}
	return nextProcessors, nil
}

// GetTriggerProcessorsForFlow retrieves the trigger processors for the specified flow.
func (fm *InMemoryFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if _, exists := fm.flows[flowID]; !exists {
		return nil, ErrFlowNotFound
	}

	triggerProcessors := make([]*definitions.SimpleTriggerProcessor, 0)
	for _, triggerProcessor := range fm.flowToTriggerProcessor[flowID] {
		triggerProcessors = append(triggerProcessors, triggerProcessor)
	}
	return triggerProcessors, nil
}

// ListFlows lists flows with pagination and a time filter.
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

// GetFlowByID retrieves a flow by its unique identifier.
func (fm *InMemoryFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}
	return flow, nil
}

// GetProcessorByID retrieves a processor by its ID within a flow.
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

// AddProcessorToFlowBefore adds a processor before the reference processor by adjusting NextProcessorIDs.
func (fm *InMemoryFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// validate reference processor exists
	_, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}

	// set new processor's NextProcessorIDs to point to the referenceProcessor
	processor.NextProcessorIDs = []uuid.UUID{referenceProcessorID}

	// find processors that point to the referenceProcessor and redirect them to the new processor
	for _, proc := range fm.flowToProcessor[flowID] {
		for i, nextID := range proc.NextProcessorIDs {
			if nextID == referenceProcessorID {
				proc.NextProcessorIDs[i] = processor.ID
			}
		}
	}

	// add the new processor to the flow
	if fm.flowToProcessor[flowID] == nil {
		fm.flowToProcessor[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	fm.flowToProcessor[flowID][processor.ID] = processor
	fm.flowUpdates[flowID] = time.Now()
	return nil
}

// AddProcessorToFlowAfter adds a processor after the reference processor.
func (fm *InMemoryFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}

	// Add the new processor after the reference processor by updating NextProcessorIDs
	referenceProcessor.NextProcessorIDs = append(referenceProcessor.NextProcessorIDs, processor.ID)

	// Add the processor to the flow
	if fm.flowToProcessor[flowID] == nil {
		fm.flowToProcessor[flowID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	fm.flowToProcessor[flowID][processor.ID] = processor
	fm.flowUpdates[flowID] = time.Now()
	return nil
}

// SaveFlow saves the flow and its processors.
func (fm *InMemoryFlowManager) SaveFlow(flow *definitions.Flow) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.flows[flow.ID] = flow
	if fm.flowToProcessor[flow.ID] == nil {
		fm.flowToProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	for _, processor := range flow.Processors {
		fm.flowToProcessor[flow.ID][processor.ID] = processor
	}
	if fm.flowToTriggerProcessor[flow.ID] == nil {
		fm.flowToTriggerProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleTriggerProcessor)
	}
	for _, triggerProcessor := range flow.TriggerProcessors {
		fm.flowToTriggerProcessor[flow.ID][triggerProcessor.ID] = triggerProcessor
	}
	fm.flowUpdates[flow.ID] = time.Now()
	return nil
}

// GetLastUpdateTime retrieves the last update time for the given flow IDs.
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

// SetFlowActive marks a flow as active or inactive.
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
