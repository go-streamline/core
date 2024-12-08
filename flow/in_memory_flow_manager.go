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

// GetFirstProcessorsForFlow retrieves processors that are not referenced as NextProcessors by any other processor.
func (fm *InMemoryFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	// Build a set of processors that are referenced as NextProcessors
	referenced := make(map[uuid.UUID]bool)
	for _, p := range flow.Processors {
		for _, np := range p.NextProcessors {
			referenced[np.ID] = true
		}
	}

	var firstProcessors []*definitions.SimpleProcessor
	for _, p := range flow.Processors {
		if !referenced[p.ID] {
			firstProcessors = append(firstProcessors, p)
		}
	}

	return firstProcessors, nil
}

// GetFlowProcessors retrieves all processors for a given flow.
func (fm *InMemoryFlowManager) GetFlowProcessors(flowID uuid.UUID) ([]*definitions.SimpleProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	return flow.Processors, nil
}

// GetProcessors retrieves processors by their unique identifiers.
// Since we have multiple flows, we scan them all.
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

// GetNextProcessors retrieves the NextProcessors of the specified processor.
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

	return processor.NextProcessors, nil
}

// GetTriggerProcessorsForFlow retrieves the trigger processors for the specified flow.
func (fm *InMemoryFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return nil, ErrFlowNotFound
	}

	return flow.TriggerProcessors, nil
}

// ListFlows lists flows with pagination and a time filter.
func (fm *InMemoryFlowManager) ListFlows(pagination *definitions.PaginationRequest, since time.Time) (*definitions.PaginatedData[*definitions.Flow], error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	var flows []*definitions.Flow
	for _, flow := range fm.flows {
		update, exists := fm.flowUpdates[flow.ID]
		if exists && update.After(since) {
			flows = append(flows, flow)
		}
	}

	totalCount := len(flows)
	start := pagination.Offset()
	end := start + pagination.Limit()

	if start > totalCount {
		return &definitions.PaginatedData[*definitions.Flow]{
			Data:       []*definitions.Flow{},
			TotalCount: totalCount,
		}, nil
	}

	if end > totalCount {
		end = totalCount
	}

	return &definitions.PaginatedData[*definitions.Flow]{
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

// AddProcessorToFlowBefore inserts a processor before the reference processor.
// This means the new processor should now appear in place of the reference processor wherever it was referenced.
// Also, the new processor points to the reference processor in its NextProcessors.
func (fm *InMemoryFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, newProcessor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return ErrFlowNotFound
	}

	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}

	// Insert newProcessor so that wherever referenceProcessor was referenced, it gets replaced by newProcessor
	for _, p := range flow.Processors {
		for i, np := range p.NextProcessors {
			if np.ID == referenceProcessor.ID {
				p.NextProcessors[i] = newProcessor
			}
		}
	}

	// Now newProcessor should point to the referenceProcessor
	newProcessor.NextProcessors = []*definitions.SimpleProcessor{referenceProcessor}

	// Add newProcessor to the flow
	flow.Processors = append(flow.Processors, newProcessor)
	if fm.flowToProcessor[flow.ID] == nil {
		fm.flowToProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	fm.flowToProcessor[flow.ID][newProcessor.ID] = newProcessor

	fm.flowUpdates[flowID] = time.Now()
	return nil
}

// AddProcessorToFlowAfter adds a processor after the reference processor by appending it to the reference processor's NextProcessors.
func (fm *InMemoryFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, newProcessor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	flow, exists := fm.flows[flowID]
	if !exists {
		return ErrFlowNotFound
	}

	referenceProcessor, err := fm.GetProcessorByID(flowID, referenceProcessorID)
	if err != nil {
		return err
	}

	// Add newProcessor to the referenceProcessor's NextProcessors
	referenceProcessor.NextProcessors = append(referenceProcessor.NextProcessors, newProcessor)

	// Also add the newProcessor to the flow
	flow.Processors = append(flow.Processors, newProcessor)
	if fm.flowToProcessor[flow.ID] == nil {
		fm.flowToProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	fm.flowToProcessor[flow.ID][newProcessor.ID] = newProcessor

	fm.flowUpdates[flowID] = time.Now()
	return nil
}

// SaveFlow saves the flow and its processors.
// Because the flow already has Processors and TriggerProcessors with proper references, we just store them.
// We assume that `flow.Processors` includes all processors and `flow.FirstProcessors` is derived, not stored explicitly.
func (fm *InMemoryFlowManager) SaveFlow(flow *definitions.Flow) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.flows[flow.ID] = flow
	if fm.flowToProcessor[flow.ID] == nil {
		fm.flowToProcessor[flow.ID] = make(map[uuid.UUID]*definitions.SimpleProcessor)
	}
	// Store all processors in a map by ID for quick lookup
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
