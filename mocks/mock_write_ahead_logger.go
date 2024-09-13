package enginetests

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/stretchr/testify/mock"
)

type MockWriteAheadLogger struct {
	mock.Mock
}

func (m *MockWriteAheadLogger) WriteEntry(entry definitions.LogEntry) {
	m.Called(entry)
}

func (m *MockWriteAheadLogger) ReadEntries() ([]definitions.LogEntry, error) {
	args := m.Called()
	return args.Get(0).([]definitions.LogEntry), args.Error(1)
}

func (m *MockWriteAheadLogger) ReadLastEntries() ([]definitions.LogEntry, error) {
	args := m.Called()
	return args.Get(0).([]definitions.LogEntry), args.Error(1)
}
