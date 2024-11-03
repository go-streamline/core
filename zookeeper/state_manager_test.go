package zookeeper

import (
	"encoding/json"
	"errors"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// MockZKConnection simulates a Zookeeper connection for testing purposes.
type MockZKConnection struct {
	data        map[string][]byte
	existsMap   map[string]bool
	watchChans  map[string]chan zk.Event
	mu          sync.Mutex
	createError error
	setError    error
	getError    error
	getWError   error
	existsError error
}

// NewMockZKConnection initializes a new MockZKConnection.
func NewMockZKConnection() *MockZKConnection {
	return &MockZKConnection{
		data:       make(map[string][]byte),
		existsMap:  make(map[string]bool),
		watchChans: make(map[string]chan zk.Event),
	}
}

func (m *MockZKConnection) Exists(path string) (bool, *zk.Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.existsError != nil {
		return false, nil, m.existsError
	}
	exists, ok := m.existsMap[path]
	if !ok {
		return false, nil, nil
	}
	return exists, &zk.Stat{}, nil
}

func (m *MockZKConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createError != nil {
		return "", m.createError
	}
	if _, exists := m.existsMap[path]; exists {
		return "", zk.ErrNodeExists
	}
	m.existsMap[path] = true
	m.data[path] = data
	return path, nil
}

func (m *MockZKConnection) Get(path string) ([]byte, *zk.Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getError != nil {
		return nil, nil, m.getError
	}
	data, exists := m.data[path]
	if !exists {
		return nil, nil, zk.ErrNoNode
	}
	return data, &zk.Stat{}, nil
}

func (m *MockZKConnection) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setError != nil {
		return nil, m.setError
	}
	if _, exists := m.existsMap[path]; !exists {
		return nil, zk.ErrNoNode
	}
	m.data[path] = data

	// Trigger watch event if there's a watcher on this path
	if ch, ok := m.watchChans[path]; ok {
		ch <- zk.Event{Type: zk.EventNodeDataChanged, Path: path}
	}

	return &zk.Stat{}, nil
}

func (m *MockZKConnection) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getWError != nil {
		return nil, nil, nil, m.getWError
	}
	data, exists := m.data[path]
	if !exists {
		return nil, nil, nil, zk.ErrNoNode
	}
	ch := make(chan zk.Event, 1)
	m.watchChans[path] = ch
	return data, &zk.Stat{}, ch, nil
}

// Adjusted Tests

func TestNewStateManager(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	assert.NotNil(t, manager, "Expected non-nil StateManager")

	sm, ok := manager.(*stateManager)
	assert.True(t, ok, "Expected manager to be of type *stateManager")

	expectedZkPath := path.Dir(path.Join(zkPath, id.String()) + "/")
	assert.Equal(t, expectedZkPath, sm.zkPath, "Expected zkPath to be %s, got %s", expectedZkPath, sm.zkPath)

	assert.Equal(t, mockZkConn, sm.zkConnection, "Expected zkConnection to be the mockZkConn")

	assert.NotNil(t, sm.ctx, "Expected non-nil context")
	assert.NotNil(t, sm.closer, "Expected non-nil closer")
}

func TestStateManager_GetSetState_Cluster(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	sm := manager.(*stateManager)

	// Set state in cluster
	stateData := map[string]interface{}{
		"clusterKey": "clusterValue",
	}

	err := manager.SetState(definitions.StateTypeCluster, stateData)
	assert.NoError(t, err, "SetState returned an error")

	// Check that data is stored in mockZkConn
	dataBytes, ok := mockZkConn.data[sm.zkPath]
	assert.True(t, ok, "Expected data at path %s", sm.zkPath)

	var retrievedState map[string]interface{}
	err = json.Unmarshal(dataBytes, &retrievedState)
	assert.NoError(t, err, "Failed to unmarshal data from mockZkConn")

	assert.Equal(t, stateData, retrievedState, "Expected retrieved state to match stateData")

	// Get state from cluster
	retrievedState, err = manager.GetState(definitions.StateTypeCluster)
	assert.NoError(t, err, "GetState returned an error")

	assert.Equal(t, stateData, retrievedState, "Expected retrieved state to match stateData")
}

func TestStateManager_WatchState_Cluster(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	var wg sync.WaitGroup
	wg.Add(1)

	callbackInvoked := false
	callback := func() {
		callbackInvoked = true
		wg.Done()
	}

	err := manager.WatchState(definitions.StateTypeCluster, callback)
	assert.NoError(t, err, "WatchState returned an error")

	// Modify the cluster state after a short delay
	go func() {
		time.Sleep(1 * time.Second)
		stateData := map[string]interface{}{
			"clusterKey": "newValue",
		}
		err := manager.SetState(definitions.StateTypeCluster, stateData)
		assert.NoError(t, err, "SetState returned an error")
	}()

	// Wait for the callback to be invoked
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Expected
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for WatchState callback")
	}

	assert.True(t, callbackInvoked, "Expected callback to be invoked")
}

func TestStateManager_Errors(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	// Test unsupported state type
	err := manager.SetState("Unknown", nil)
	assert.ErrorIs(t, err, ErrUnsupportedStateType, "Expected ErrUnsupportedStateType")

	_, err = manager.GetState("Unknown")
	assert.ErrorIs(t, err, ErrUnsupportedStateType, "Expected ErrUnsupportedStateType")

	err = manager.WatchState("Unknown", func() {})
	assert.ErrorIs(t, err, ErrUnsupportedStateType, "Expected ErrUnsupportedStateType")
}

func TestStateManager_CorruptedClusterState(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	sm := manager.(*stateManager)

	// Simulate corrupted data in Zookeeper
	mockZkConn.data[sm.zkPath] = []byte("corrupted data")
	mockZkConn.existsMap[sm.zkPath] = true

	_, err := manager.GetState(definitions.StateTypeCluster)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrCouldNotUnmarshalState.Error(), "Expected ErrCouldNotUnmarshalState")
}

func TestStateManager_validatePathExists(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/validate/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	sm := manager.(*stateManager)

	err := sm.validatePathExists()
	assert.NoError(t, err, "validatePathExists returned an error")

	// Verify that the path exists in mockZkConn
	parts := splitPath(sm.zkPath)
	currentPath := ""
	for _, part := range parts {
		currentPath += "/" + part
		exists, _, err := mockZkConn.Exists(currentPath)
		assert.NoError(t, err, "Exists returned an error")
		assert.True(t, exists, "Expected path %s to exist", currentPath)
	}
}

func TestStateManager_validatePathExists_Error(t *testing.T) {
	// Create a mock zkConnection that returns an error on Create
	mockZkConn := NewMockZKConnection()
	mockZkConn.createError = errors.New("mock create error")

	zkPath := "/test/validate/error"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	sm := manager.(*stateManager)

	err := sm.validatePathExists()
	assert.Error(t, err, "Expected error from validatePathExists")
	assert.Contains(t, err.Error(), ErrCouldNotCreatePath.Error(), "Expected ErrCouldNotCreatePath")
}

func TestStateManager_ContextCancellation(t *testing.T) {
	mockZkConn := NewMockZKConnection()

	zkPath := "/test/path"
	id := uuid.New()

	manager := NewStateManager(mockZkConn, zkPath, id)

	sm := manager.(*stateManager)

	callbackCh := make(chan struct{}, 1)

	callbackInvoked := false
	callback := func() {
		callbackInvoked = true
		callbackCh <- struct{}{}
	}

	err := manager.WatchState(definitions.StateTypeCluster, callback)
	assert.NoError(t, err, "WatchState returned an error")

	// Cancel the context
	sm.closer()

	// Modify the cluster state after a short delay
	go func() {
		time.Sleep(1 * time.Second)
		stateData := map[string]interface{}{
			"clusterKey": "newValue",
		}
		err := manager.SetState(definitions.StateTypeCluster, stateData)
		assert.NoError(t, err, "SetState returned an error")
	}()

	// Wait for a short time to ensure callback is not invoked
	select {
	case <-time.After(3 * time.Second):
		// Expected
	case <-callbackCh:
		// Callback invoked, which is not expected
		t.Error("Callback should not be invoked after context cancellation")
	}

	assert.False(t, callbackInvoked, "Callback should not be invoked after context cancellation")
}
