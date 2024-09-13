package enginetests

import (
	"time"

	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type MockFlowManager struct {
	mock.Mock
}

func (m *MockFlowManager) GetFirstProcessorsForFlow(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	args := m.Called(flowID)
	return args.Get(0).([]definitions.SimpleProcessor), args.Error(1)
}

func (m *MockFlowManager) GetFlowProcessors(flowID uuid.UUID) ([]definitions.SimpleProcessor, error) {
	args := m.Called(flowID)
	return args.Get(0).([]definitions.SimpleProcessor), args.Error(1)
}

func (m *MockFlowManager) GetTriggerProcessorsForFlow(flowID uuid.UUID) ([]*definitions.SimpleTriggerProcessor, error) {
	args := m.Called(flowID)
	return args.Get(0).([]*definitions.SimpleTriggerProcessor), args.Error(1)
}

func (m *MockFlowManager) GetProcessors(processorIDs []uuid.UUID) ([]definitions.SimpleProcessor, error) {
	args := m.Called(processorIDs)
	return args.Get(0).([]definitions.SimpleProcessor), args.Error(1)
}

func (m *MockFlowManager) ListFlows(pagination *definitions.PaginationRequest, since time.Time) (definitions.PaginatedData[*definitions.Flow], error) {
	args := m.Called(pagination, since)
	return args.Get(0).(definitions.PaginatedData[*definitions.Flow]), args.Error(1)
}

func (m *MockFlowManager) GetFlowByID(flowID uuid.UUID) (*definitions.Flow, error) {
	args := m.Called(flowID)
	return args.Get(0).(*definitions.Flow), args.Error(1)
}

func (m *MockFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*definitions.SimpleProcessor, error) {
	args := m.Called(flowID, processorID)
	return args.Get(0).(*definitions.SimpleProcessor), args.Error(1)
}

func (m *MockFlowManager) AddProcessorToFlowBefore(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	args := m.Called(flowID, processor, referenceProcessorID)
	return args.Error(0)
}

func (m *MockFlowManager) AddProcessorToFlowAfter(flowID uuid.UUID, processor *definitions.SimpleProcessor, referenceProcessorID uuid.UUID) error {
	args := m.Called(flowID, processor, referenceProcessorID)
	return args.Error(0)
}

func (m *MockFlowManager) SaveFlow(flow *definitions.Flow) error {
	args := m.Called(flow)
	return args.Error(0)
}

func (m *MockFlowManager) GetLastUpdateTime(flowIDs []uuid.UUID) (map[uuid.UUID]time.Time, error) {
	args := m.Called(flowIDs)
	return args.Get(0).(map[uuid.UUID]time.Time), args.Error(1)
}

func (m *MockFlowManager) SetFlowActive(flowID uuid.UUID, active bool) error {
	args := m.Called(flowID, active)
	return args.Error(0)
}
