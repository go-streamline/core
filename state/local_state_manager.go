package state

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"time"
)

var (
	ErrCouldNotCreateDirectory = fmt.Errorf("could not create state directory")
	ErrCouldNotOpenFile        = fmt.Errorf("could not open state file")
	ErrCouldNotReadFile        = fmt.Errorf("could not read state file")
	ErrFileCorrupted           = fmt.Errorf("state file corrupted, could not unmarshal")
	ErrCouldNotWriteFile       = fmt.Errorf("could not write state file")
	ErrOnlyLocalStateSupported = fmt.Errorf("only local state is supported")
)

type localStateManager struct {
	path   string
	id     uuid.UUID
	ctx    context.Context
	closer context.CancelFunc
}

func NewLocalStateManager(statePath string, id uuid.UUID) definitions.StateManager {
	ctx, closer := context.WithCancel(context.Background())
	// if path does not end with /, add it
	if statePath[len(statePath)-1] != '/' {
		statePath = statePath + "/"
	}
	return &localStateManager{
		path:   path.Dir(statePath),
		id:     id,
		ctx:    ctx,
		closer: closer,
	}
}

func (s *localStateManager) Reset() error {
	if s.closer != nil {
		s.closer()
	}
	s.ctx, s.closer = context.WithCancel(context.Background())
	return nil
}

func (s *localStateManager) Close() error {
	if s.closer != nil {
		s.closer()
	}
	return nil
}

func (s *localStateManager) GetState(stateType definitions.StateType) (map[string]any, error) {
	if stateType != definitions.StateTypeLocal {
		return nil, ErrOnlyLocalStateSupported
	}

	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		err = os.MkdirAll(s.path, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCouldNotCreateDirectory, err)
		}
	}

	fullPath := path.Join(s.path, s.id.String())
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %v", ErrCouldNotOpenFile, err)
	}
	fileStat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCouldNotReadFile, err)
	}
	defer file.Close()
	value := make([]byte, fileStat.Size())
	_, err = file.Read(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCouldNotReadFile, err)
	}

	if len(value) == 0 {
		return nil, nil
	}

	var state map[string]any

	err = json.Unmarshal(value, &state)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFileCorrupted, err)
	}

	return state, nil
}

func (s *localStateManager) SetState(stateType definitions.StateType, value map[string]any) error {
	if stateType != definitions.StateTypeLocal {
		return ErrOnlyLocalStateSupported
	}

	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		err = os.MkdirAll(s.path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCouldNotCreateDirectory, err)
		}
	}

	fullPath := path.Join(s.path, s.id.String())
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCouldNotOpenFile, err)
	}

	defer file.Close()

	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCouldNotReadFile, err)
	}

	_, err = file.Write(bytes)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCouldNotWriteFile, err)
	}

	return nil
}

func (s *localStateManager) WatchState(state definitions.StateType, callback func()) error {
	if state != definitions.StateTypeLocal {
		return ErrOnlyLocalStateSupported
	}

	fullPath := path.Join(s.path, s.id.String())
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		err = os.MkdirAll(s.path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCouldNotCreateDirectory, err)
		}
	}

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		file, err := os.Create(fullPath)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCouldNotOpenFile, err)
		}
		file.Close()
	}

	go s.watchFile(fullPath, callback)
	return nil
}

func (s *localStateManager) watchFile(filename string, callback func()) {
	var lastModTime time.Time

	for {
		select {
		case <-time.After(5 * time.Second):
			info, err := os.Stat(filename)
			if err != nil {
				logrus.Warnf("Could not stat file: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if !info.ModTime().Equal(lastModTime) {
				fmt.Printf("File modified: %s\n", filename)
				lastModTime = info.ModTime()
				defer callback()
				return
			}

		case <-s.ctx.Done():
			return
		}
	}
}
