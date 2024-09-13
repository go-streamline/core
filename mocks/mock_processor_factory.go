package mocks

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type MockProcessorFactory struct {
	mock.Mock
}

func (m *MockProcessorFactory) GetProcessor(id uuid.UUID, typeName string) (definitions.Processor, error) {
	args := m.Called(id, typeName)
	return args.Get(0).(definitions.Processor), args.Error(1)
}

func (m *MockProcessorFactory) GetTriggerProcessor(id uuid.UUID, typeName string) (definitions.TriggerProcessor, error) {
	args := m.Called(id, typeName)
	return args.Get(0).(definitions.TriggerProcessor), args.Error(1)
}
