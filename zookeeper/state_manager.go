package zookeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"path"
)

var (
	ErrCouldNotCreatePath     = fmt.Errorf("could not create path")
	ErrCouldNotUnmarshalState = fmt.Errorf("could not unmarshal state")
	ErrCouldNotMarshalState   = fmt.Errorf("could not marshal state")
	ErrCouldNotSaveState      = fmt.Errorf("could not save state")
	ErrUnsupportedStateType   = fmt.Errorf("only CLUSTER and LOCAL state types are supported")
	ErrFailedToWatchStatePath = fmt.Errorf("failed to watch state path in zookeeper")
)

type stateManager struct {
	zkConnection localStateManagerZookeeper
	zkPath       string
	ctx          context.Context
	closer       context.CancelFunc
}

type localStateManagerZookeeper interface {
	zkCreateFullPathInterface
	Get(path string) ([]byte, *zk.Stat, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
}

func NewStateManager(
	zkConnection localStateManagerZookeeper,
	zkPath string,
	id uuid.UUID,
) definitions.StateManager {
	ctx, closer := context.WithCancel(context.Background())
	return &stateManager{
		zkConnection: zkConnection,
		zkPath:       path.Dir(path.Join(zkPath, id.String()) + "/"),
		ctx:          ctx,
		closer:       closer,
	}
}

func (s *stateManager) Close() error {
	if s.closer != nil {
		s.closer()
	}
	return nil
}

func (s *stateManager) Reset() error {
	if s.closer != nil {
		s.closer()
	}
	s.ctx, s.closer = context.WithCancel(context.Background())
	return nil
}

func (s *stateManager) GetState(stateType definitions.StateType) (map[string]any, error) {
	if stateType == definitions.StateTypeCluster {
		err := s.validatePathExists()
		if err != nil {
			return nil, err
		}

		data, _, err := s.zkConnection.Get(s.zkPath)
		if err != nil {
			return nil, err
		}

		if len(data) == 0 {
			return nil, nil
		}

		var value map[string]any
		err = json.Unmarshal(data, &value)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCouldNotUnmarshalState, err)
		}

		return value, nil
	} else {
		return nil, ErrUnsupportedStateType
	}
}

func (s *stateManager) SetState(stateType definitions.StateType, value map[string]any) error {
	if stateType == definitions.StateTypeCluster {

		err := s.validatePathExists()
		if err != nil {
			return err
		}

		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCouldNotMarshalState, err)
		}

		_, err = s.zkConnection.Set(s.zkPath, data, -1)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCouldNotSaveState, err)
		}

		return nil
	} else {
		return ErrUnsupportedStateType
	}
}

func (s *stateManager) WatchState(stateType definitions.StateType, callback func()) error {
	if stateType == definitions.StateTypeCluster {
		err := s.validatePathExists()
		if err != nil {
			return err
		}

		_, _, ch, err := s.zkConnection.GetW(s.zkPath)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrFailedToWatchStatePath, err)
		}

		go func() {
			select {
			case <-s.ctx.Done():
				return
			case <-ch:
				callback()
			}
		}()
		return nil
	} else {
		return ErrUnsupportedStateType
	}
}

func (s *stateManager) validatePathExists() error {
	_, _, err := CreateFullPath(s.zkConnection, s.zkPath, []byte{}, zk.FlagPersistent)
	if err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return fmt.Errorf("%w: %v", ErrCouldNotCreatePath, err)
	}
	return nil
}
