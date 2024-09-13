package mocks

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/stretchr/testify/mock"
	"io"
)

type MockFileHandler struct {
	mock.Mock
	definitions.EngineFileHandler
}

func (m *MockFileHandler) GetInputFile() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockFileHandler) GetOutputFile() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockFileHandler) Read() (io.Reader, error) {
	args := m.Called()
	return args.Get(0).(io.Reader), args.Error(1)
}

func (m *MockFileHandler) Write() (io.Writer, error) {
	args := m.Called()
	return args.Get(0).(io.Writer), args.Error(1)
}

func (m *MockFileHandler) Close() {
	m.Called()
}

func (m *MockFileHandler) GenerateNewFileHandler() (definitions.EngineFileHandler, error) {
	args := m.Called()
	return args.Get(0).(definitions.EngineFileHandler), args.Error(1)
}
