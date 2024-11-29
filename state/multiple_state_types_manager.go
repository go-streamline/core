package state

import (
	"errors"
	"github.com/go-streamline/interfaces/definitions"
)

var (
	ErrUnsupportedStateType = errors.New("unsupported state type")
)

type MultipleStateTypesManager struct {
	stateManagers map[definitions.StateType]definitions.StateManager
}

func NewMultipleStateTypesManager(stateManagers map[definitions.StateType]definitions.StateManager) definitions.StateManager {
	return &MultipleStateTypesManager{
		stateManagers: stateManagers,
	}
}

func (m *MultipleStateTypesManager) Close() error {
	for _, stateManager := range m.stateManagers {
		_ = stateManager.Close()
	}
	return nil
}

func (m *MultipleStateTypesManager) Reset() error {
	for _, stateManager := range m.stateManagers {
		_ = stateManager.Reset()
	}
	return nil
}

func (m *MultipleStateTypesManager) GetState(stateType definitions.StateType) (map[string]any, error) {
	stateManager, ok := m.stateManagers[stateType]
	if !ok {
		return nil, ErrUnsupportedStateType
	}
	stateMap, err := stateManager.GetState(stateType)
	if err != nil {
		return nil, err
	}
	if stateMap == nil {
		return map[string]any{}, nil
	}
	return stateMap, nil
}

func (m *MultipleStateTypesManager) SetState(stateType definitions.StateType, state map[string]any) error {
	stateManager, ok := m.stateManagers[stateType]
	if !ok {
		return ErrUnsupportedStateType
	}
	return stateManager.SetState(stateType, state)
}

func (m *MultipleStateTypesManager) WatchState(state definitions.StateType, callback func()) error {
	stateManager, ok := m.stateManagers[state]
	if !ok {
		return ErrUnsupportedStateType
	}
	return stateManager.WatchState(state, callback)
}

func (m *MultipleStateTypesManager) GetStateManagers() map[definitions.StateType]definitions.StateManager {
	return m.stateManagers
}
