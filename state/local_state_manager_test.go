package state

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
)

func TestNewLocalStateManager(t *testing.T) {
	statePath := "./test_state/"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)

	assert.NotNil(t, manager, "Expected non-nil StateManager")

	lsm, ok := manager.(*localStateManager)
	assert.True(t, ok, "Expected manager to be of type *localStateManager")

	expectedPath := path.Dir(statePath)
	assert.True(t, lsm.path == expectedPath, "Expected path %s, got %s", expectedPath, lsm.path)

	assert.True(t, lsm.id == id, "Expected ID %s, got %s", id, lsm.id)

	assert.NotNil(t, lsm.ctx, "Expected non-nil context")
	assert.NotNil(t, lsm.closer, "Expected non-nil closer")
}

func TestLocalStateManager_Reset(t *testing.T) {
	statePath := "./test_state"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	lsm := manager.(*localStateManager)

	err := lsm.Reset()
	assert.NoError(t, err, "Reset returned an error: ", err)

	select {
	case <-lsm.ctx.Done():
		t.Error("Context should not be canceled after reset")
	default:
		// Expected path
	}
}

func TestLocalStateManager_Close(t *testing.T) {
	statePath := "./test_state"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	lsm := manager.(*localStateManager)

	err := lsm.Close()
	assert.NoError(t, err, "Close returned an error: ", err)

	select {
	case <-lsm.ctx.Done():
		// Expected path
	default:
		t.Error("Context should be canceled after close")
	}
}

func TestLocalStateManager_GetSetState(t *testing.T) {
	statePath := "./test_state_getset"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	// Test SetState
	stateData := map[string]any{
		"key1": "value1",
		"key2": 42,
	}

	err := manager.SetState(definitions.StateTypeLocal, stateData)
	assert.NoError(t, err, "SetState returned an error: ", err)

	// Test GetState
	retrievedState, err := manager.GetState(definitions.StateTypeLocal)
	assert.NoError(t, err, "GetState returned an error: ", err)

	originalJSON, err := json.Marshal(stateData)
	assert.NoError(t, err, "Failed to marshal original state: ", err)

	retrievedJSON, err := json.Marshal(retrievedState)
	assert.NoError(t, err, "Failed to marshal retrieved state: ", err)

	assert.Equal(t, string(originalJSON), string(retrievedJSON))

	// Test GetState when file does not exist
	err = os.RemoveAll(statePath)
	assert.NoError(t, err, "Failed to remove state file: ", err)
	retrievedState, err = manager.GetState(definitions.StateTypeLocal)
	assert.NoError(t, err, "GetState returned an error: ", err)

	assert.Nil(t, retrievedState, "Expected nil state when file does not exist")
}

func TestLocalStateManager_WatchState(t *testing.T) {
	statePath := "./test_state_watch"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	var wg sync.WaitGroup
	wg.Add(1)

	callbackInvoked := false
	callback := func() {
		callbackInvoked = true
		wg.Done()
	}

	err := manager.WatchState(definitions.StateTypeLocal, callback)
	assert.NoError(t, err, "WatchState returned an error: ", err)

	// Modify the state file after a short delay
	go func() {
		time.Sleep(1 * time.Second)
		stateData := map[string]any{
			"updatedKey": "updatedValue",
		}
		err := manager.SetState(definitions.StateTypeLocal, stateData)
		assert.NoError(t, err, "SetState returned an error: ", err)
	}()

	// Wait for the callback to be invoked
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Expected path
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for WatchState callback")
	}

	if !callbackInvoked {
		t.Error("Expected callback to be invoked")
	}
}

func TestLocalStateManager_Errors(t *testing.T) {
	statePath := "./test_state_errors"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	// Test SetState with unsupported state type
	err := manager.SetState(definitions.StateTypeCluster, nil)
	assert.ErrorIs(t, err, ErrOnlyLocalStateSupported)

	// Test GetState with unsupported state type
	_, err = manager.GetState(definitions.StateTypeCluster)
	assert.ErrorIs(t, err, ErrOnlyLocalStateSupported)

	// Test WatchState with unsupported state type
	err = manager.WatchState(definitions.StateTypeCluster, func() {})
	assert.ErrorIs(t, err, ErrOnlyLocalStateSupported)
}

func TestLocalStateManager_CorruptedFile(t *testing.T) {
	statePath := "./test_state_corrupt/"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	// Create a corrupted state file
	fullPath := filepath.Join(path.Dir(statePath), id.String())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	assert.NoError(t, err, "Failed to create directory: ", err)

	file, err := os.Create(fullPath)
	assert.NoError(t, err, "Failed to create file: ", err)

	_, err = file.Write([]byte("corrupted data"))
	assert.NoError(t, err, "Failed to write to file: ", err)
	file.Close()

	// Attempt to GetState
	_, err = manager.GetState(definitions.StateTypeLocal)
	if err == nil || err.Error() != ErrFileCorrupted.Error()+": invalid character 'c' looking for beginning of value" {
		t.Errorf("Expected error starting with %v, got %v", ErrFileCorrupted, err)
	}
}

func TestLocalStateManager_ContextCancellation(t *testing.T) {
	statePath := "./test_state_context"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	lsm := manager.(*localStateManager)

	var wg sync.WaitGroup
	wg.Add(1)

	callbackInvoked := false
	callback := func() {
		callbackInvoked = true
		wg.Done()
	}

	err := lsm.WatchState(definitions.StateTypeLocal, callback)
	assert.NoError(t, err, "WatchState returned an error: ", err)

	// Cancel the context to stop the watcher
	lsm.closer()

	// Wait to ensure watcher has exited
	time.Sleep(2 * time.Second)

	// Modify the state file
	stateData := map[string]any{
		"key": "value",
	}
	err = lsm.SetState(definitions.StateTypeLocal, stateData)
	assert.NoError(t, err, "SetState returned an error: ", err)

	// Callback should not be invoked
	select {
	case <-time.After(3 * time.Second):
		// Expected path
	}

	assert.False(t, callbackInvoked, "Callback should not be invoked after context cancellation")
}

func TestLocalStateManager_MultipleWatchers(t *testing.T) {
	statePath := "./test_state_multiple_watchers"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	var wg sync.WaitGroup
	wg.Add(2)

	callback1Invoked := false
	callback1 := func() {
		callback1Invoked = true
		wg.Done()
	}

	callback2Invoked := false
	callback2 := func() {
		callback2Invoked = true
		wg.Done()
	}

	err := manager.WatchState(definitions.StateTypeLocal, callback1)
	assert.NoError(t, err, "WatchState returned an error: ", err)

	err = manager.WatchState(definitions.StateTypeLocal, callback2)
	assert.NoError(t, err, "WatchState returned an error: ", err)

	// Modify the state file
	go func() {
		time.Sleep(1 * time.Second)
		stateData := map[string]any{
			"key": "value",
		}
		err := manager.SetState(definitions.StateTypeLocal, stateData)
		assert.NoError(t, err, "SetState returned an error: ", err)
	}()

	// Wait for callbacks to be invoked
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Expected path
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for WatchState callbacks")
	}

	assert.False(t, !callback1Invoked || !callback2Invoked, "Expected both callbacks to be invoked")
}

func TestLocalStateManager_InvalidJSON(t *testing.T) {
	statePath := "./test_state_invalid_json/"
	id := uuid.New()
	manager := NewLocalStateManager(statePath, id)
	defer os.RemoveAll(statePath)

	// Write invalid JSON to the state file
	fullPath := filepath.Join(path.Dir(statePath), id.String())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	assert.NoError(t, err, "Failed to create directory: ", err)

	err = os.WriteFile(fullPath, []byte("{invalid json"), 0644)
	assert.NoError(t, err, "Failed to write to file: ", err)

	// Attempt to GetState
	_, err = manager.GetState(definitions.StateTypeLocal)
	assert.ErrorIs(t, err, ErrFileCorrupted)
}
