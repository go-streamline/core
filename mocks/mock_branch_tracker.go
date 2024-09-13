package mocks

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type MockBranchTracker struct {
	mock.Mock
}

func (m *MockBranchTracker) GetBranches() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockBranchTracker) AddProcessor(sessionID uuid.UUID, processorID uuid.UUID, nextProcessorIDs []uuid.UUID) {
	m.Called(sessionID, processorID, nextProcessorIDs)
}

// MarkComplete mark the processor as completed
func (m *MockBranchTracker) MarkComplete(sessionID uuid.UUID, processorID uuid.UUID) (allComplete bool) {
	args := m.Called(sessionID, processorID)
	return args.Bool(0)
}

// IsComplete check if all processors in a session are completed
func (m *MockBranchTracker) IsComplete(sessionID uuid.UUID) bool {
	args := m.Called(sessionID)
	return args.Bool(0)
}

func (m *MockBranchTracker) RestoreState(sessionID uuid.UUID, completedProcessorIDs []uuid.UUID) {
	m.Called(sessionID, completedProcessorIDs)
}

func (m *MockBranchTracker) GetCompletedProcessors(sessionID uuid.UUID) []uuid.UUID {
	args := m.Called(sessionID)
	return args.Get(0).([]uuid.UUID)
}
