package state_test

import (
	"testing"

	"github.com/go-streamline/core/state"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/stretchr/testify/assert"
)

// MockStateManager simulates a StateManager for testing purposes.
type MockStateManager struct {
	CloseCalled      bool
	ResetCalled      bool
	GetStateCalled   bool
	SetStateCalled   bool
	WatchStateCalled bool

	GetStateStateType definitions.StateType
	SetStateStateType definitions.StateType
	WatchStateType    definitions.StateType

	SetStateValue map[string]interface{}
	WatchCallback func()

	GetStateReturnValue map[string]interface{}
	GetStateError       error
	SetStateError       error
	WatchStateError     error
	CloseError          error
	ResetError          error
}

func (m *MockStateManager) Close() error {
	m.CloseCalled = true
	return m.CloseError
}

func (m *MockStateManager) Reset() error {
	m.ResetCalled = true
	return m.ResetError
}

func (m *MockStateManager) GetState(stateType definitions.StateType) (map[string]interface{}, error) {
	m.GetStateCalled = true
	m.GetStateStateType = stateType
	return m.GetStateReturnValue, m.GetStateError
}

func (m *MockStateManager) SetState(stateType definitions.StateType, state map[string]interface{}) error {
	m.SetStateCalled = true
	m.SetStateStateType = stateType
	m.SetStateValue = state
	return m.SetStateError
}

func (m *MockStateManager) WatchState(state definitions.StateType, callback func()) error {
	m.WatchStateCalled = true
	m.WatchStateType = state
	m.WatchCallback = callback
	return m.WatchStateError
}

func TestMultipleStateTypesManager_GetState(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Set return values for mock managers
	mockManager1.GetStateReturnValue = map[string]interface{}{"key": "value1"}
	mockManager2.GetStateReturnValue = map[string]interface{}{"key": "value2"}

	// Test GetState for StateType1
	stateValue, err := manager.GetState("StateType1")
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"key": "value1"}, stateValue)
	assert.True(t, mockManager1.GetStateCalled)
	assert.Equal(t, definitions.StateType("StateType1"), mockManager1.GetStateStateType)

	// Test GetState for StateType2
	stateValue, err = manager.GetState("StateType2")
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"key": "value2"}, stateValue)
	assert.True(t, mockManager2.GetStateCalled)
	assert.Equal(t, definitions.StateType("StateType2"), mockManager2.GetStateStateType)

	// Test GetState for unsupported StateType
	stateValue, err = manager.GetState("UnsupportedStateType")
	assert.Nil(t, stateValue)
	assert.ErrorIs(t, err, state.ErrUnsupportedStateType)
}

func TestMultipleStateTypesManager_SetState(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Test SetState for StateType1
	err := manager.SetState("StateType1", map[string]interface{}{"key": "value1"})
	assert.NoError(t, err)
	assert.True(t, mockManager1.SetStateCalled)
	assert.Equal(t, definitions.StateType("StateType1"), mockManager1.SetStateStateType)
	assert.Equal(t, map[string]interface{}{"key": "value1"}, mockManager1.SetStateValue)

	// Test SetState for StateType2
	err = manager.SetState("StateType2", map[string]interface{}{"key": "value2"})
	assert.NoError(t, err)
	assert.True(t, mockManager2.SetStateCalled)
	assert.Equal(t, definitions.StateType("StateType2"), mockManager2.SetStateStateType)
	assert.Equal(t, map[string]interface{}{"key": "value2"}, mockManager2.SetStateValue)

	// Test SetState for unsupported StateType
	err = manager.SetState("UnsupportedStateType", map[string]interface{}{"key": "value"})
	assert.ErrorIs(t, err, state.ErrUnsupportedStateType)
}

func TestMultipleStateTypesManager_WatchState(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Prepare a callback function
	callback := func() {}

	// Test WatchState for StateType1
	err := manager.WatchState("StateType1", callback)
	assert.NoError(t, err)
	assert.True(t, mockManager1.WatchStateCalled)
	assert.Equal(t, definitions.StateType("StateType1"), mockManager1.WatchStateType)

	// Test WatchState for StateType2
	err = manager.WatchState("StateType2", callback)
	assert.NoError(t, err)
	assert.True(t, mockManager2.WatchStateCalled)
	assert.Equal(t, definitions.StateType("StateType2"), mockManager2.WatchStateType)

	// Test WatchState for unsupported StateType
	err = manager.WatchState("UnsupportedStateType", callback)
	assert.ErrorIs(t, err, state.ErrUnsupportedStateType)
}

func TestMultipleStateTypesManager_Close(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}
	mockManager3 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
		definitions.StateType("StateType3"): mockManager3,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Test Close
	err := manager.Close()
	assert.NoError(t, err)
	assert.True(t, mockManager1.CloseCalled)
	assert.True(t, mockManager2.CloseCalled)
	assert.True(t, mockManager3.CloseCalled)
}

func TestMultipleStateTypesManager_Reset(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}
	mockManager3 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
		definitions.StateType("StateType3"): mockManager3,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Test Reset
	err := manager.Reset()
	assert.NoError(t, err)
	assert.True(t, mockManager1.ResetCalled)
	assert.True(t, mockManager2.ResetCalled)
	assert.True(t, mockManager3.ResetCalled)
}

func TestMultipleStateTypesManager_GetStateManagers(t *testing.T) {
	// Create mock state managers
	mockManager1 := &MockStateManager{}
	mockManager2 := &MockStateManager{}

	stateManagers := map[definitions.StateType]definitions.StateManager{
		definitions.StateType("StateType1"): mockManager1,
		definitions.StateType("StateType2"): mockManager2,
	}

	manager := state.NewMultipleStateTypesManager(stateManagers)

	// Test GetStateManagers
	returnedManagers := manager.(*state.MultipleStateTypesManager).GetStateManagers()
	assert.Equal(t, stateManagers, returnedManagers)
}
