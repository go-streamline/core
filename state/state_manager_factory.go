package state

import (
	"github.com/go-streamline/core/zookeeper"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
)

type stateManagerFactory struct {
	zkClient       *zk.Conn
	localStatePath string
	zookeeperPath  string
}

func NewStateManagerFactory(zkClient *zk.Conn, localStatePath string, zookeeperPath string) definitions.StateManagerFactory {
	return &stateManagerFactory{
		zkClient:       zkClient,
		localStatePath: localStatePath,
		zookeeperPath:  zookeeperPath,
	}
}

func (f *stateManagerFactory) CreateStateManager(id uuid.UUID) definitions.StateManager {
	return NewMultipleStateTypesManager(map[definitions.StateType]definitions.StateManager{
		definitions.StateTypeLocal:   NewLocalStateManager(f.localStatePath, id),
		definitions.StateTypeCluster: zookeeper.NewStateManager(f.zkClient, f.zookeeperPath, id),
	})
}
