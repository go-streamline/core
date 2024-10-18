package zookeeper

import (
	"context"
	"errors"
	"github.com/go-streamline/interfaces/definitions"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

type leaderSelector struct {
	conn             *zk.Conn
	znodePath        string
	nodeName         string // Store the created node name
	currentLeader    string // Hostname of the current leader
	isLeader         bool
	leaderMutex      sync.Mutex
	electLeaderMutex sync.Mutex
	log              *logrus.Logger
	ctx              context.Context
	cancel           context.CancelFunc
	nodeChangeCh     chan []string // Channel to notify about node changes
}

// NewZookeeperLeaderSelector creates a new instance of the leader selector for Zookeeper.
func NewZookeeperLeaderSelector(conn *zk.Conn, znodePath string, log *logrus.Logger) definitions.LeaderSelector {
	ctx, cancel := context.WithCancel(context.Background())
	return &leaderSelector{
		conn:         conn,
		znodePath:    znodePath,
		log:          log,
		ctx:          ctx,
		cancel:       cancel,
		nodeChangeCh: make(chan []string, 1),
	}
}

// NodeName returns the name of the leader node.
func (z *leaderSelector) NodeName() string {
	return z.currentLeader
}

// NodeChangeChannel returns a channel that can be used to receive node change notifications.
func (z *leaderSelector) NodeChangeChannel() <-chan []string {
	return z.nodeChangeCh
}

// Start attempts to elect the node as a leader or monitor the current leader.
func (z *leaderSelector) Start() error {
	z.log.Infof("starting leader election on znode %s", z.znodePath)

	go z.monitorNodeChanges()
	go z.monitorConnection()
	_, err := z.tryElectLeader()

	return err
}

func (z *leaderSelector) Nodes() ([]string, error) {
	children, _, err := z.conn.Children(z.znodePath)
	if err != nil {
		return nil, err
	}

	var nodes []string
	for _, child := range children {
		if len(child) >= len("leader_") && child[:len("leader_")] == "leader_" {
			data, _, err := z.conn.Get(z.znodePath + "/" + child)
			if err != nil {
				z.log.WithError(err).Errorf("failed to retrieve data for child %s", child)
				continue
			}
			nodes = append(nodes, string(data))
		}
	}

	return nodes, nil
}

// IsLeader checks if the current node is the leader in the given zNode path.
// If the node is not a leader, it will monitor the leader status.
func (z *leaderSelector) IsLeader() (bool, error) {
	z.leaderMutex.Lock()
	defer z.leaderMutex.Unlock()
	return z.isLeader, nil
}

// Close stops the leader selector by canceling the context.
func (z *leaderSelector) Close() error {
	z.cancel()
	z.log.Infof("stopping leader selector")
	return nil
}

// createZnode creates the znode for leader election.
func (z *leaderSelector) createZnode() error {
	flags := int32(zk.FlagEphemeralSequential)
	path := z.znodePath + "/leader_"
	createdPath, err := CreateFullPath(z.conn, path, nil, flags)
	if err != nil {
		z.log.WithError(err).Error("failed to create znode for leader election")
		return err
	}
	z.nodeName = createdPath
	return nil
}

// Internal function that attempts to elect the node as a leader.
func (z *leaderSelector) tryElectLeader() (bool, error) {
	z.electLeaderMutex.Lock()
	defer z.electLeaderMutex.Unlock()

	if z.nodeName == "" {
		err := z.createZnode()
		if err != nil {
			return false, err
		}
	} else {
		exists, _, err := z.conn.Exists(z.nodeName)
		if err != nil {
			z.log.WithError(err).Error("failed to check existence of znode")
			return false, err
		}
		if !exists {
			z.log.Warnf("znode not found, attempting to create a new znode")
			err = z.createZnode()
			if err != nil {
				return false, err
			}
		}
	}

	children, _, err := z.conn.Children(z.znodePath)
	if err != nil {
		z.log.WithError(err).Error("failed to retrieve children for leader election")
		return false, err
	}

	// Filter and sort the children to find the minimum node with the correct prefix
	var eligibleChildren []string
	for _, child := range children {
		if len(child) >= len("leader_") && child[:len("leader_")] == "leader_" {
			eligibleChildren = append(eligibleChildren, child)
		}
	}

	if len(eligibleChildren) == 0 {
		return false, errors.New("no eligible leader nodes found")
	}

	minNode := eligibleChildren[0]
	for _, child := range eligibleChildren {
		if child < minNode {
			minNode = child
		}
	}

	minNodePath := z.znodePath + "/" + minNode
	data, _, err := z.conn.Get(minNodePath)
	if err != nil {
		z.log.WithError(err).Error("failed to get data for current leader")
		return false, err
	}

	z.currentLeader = string(data)

	if minNodePath == z.nodeName {
		z.updateLeaderStatus(true)
		z.log.Infof("node %s has acquired leadership", z.nodeName)
		return true, nil
	}

	z.updateLeaderStatus(false)
	return false, nil
}

// Internal function that monitors node changes and notifies through the nodeChangeCh.
func (z *leaderSelector) monitorNodeChanges() {
	for {
		children, _, ch, err := z.conn.ChildrenW(z.znodePath)
		if err != nil {
			z.log.WithError(err).Error("error occurred while monitoring node changes")
			time.Sleep(5 * time.Second)
			continue
		}

		var nodes []string
		for _, child := range children {
			if len(child) >= len("leader_") && child[:len("leader_")] == "leader_" {
				data, _, err := z.conn.Get(z.znodePath + "/" + child)
				if err != nil {
					z.log.WithError(err).Errorf("failed to retrieve data for child %s", child)
					continue
				}
				nodes = append(nodes, string(data))
			}
		}

		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged || event.Type == zk.EventNodeDeleted {
				z.log.Info("nodes changed or deleted, notifying through nodeChangeCh")
				select {
				case z.nodeChangeCh <- nodes:
					// Successfully sent notification
				default:
					// Channel is full, dropping notification to avoid infinite loop
					z.log.Warn("nodeChangeCh is full, dropping notification to prevent infinite loop")
				}
				// Re-check leadership status after nodes change
				_, _ = z.tryElectLeader()
			}
		case <-z.ctx.Done():
			return
		}
	}
}

// Internal function to handle the transition of leadership.
// When a node is elected or loses leadership, this function will update the leader status.
func (z *leaderSelector) updateLeaderStatus(isLeader bool) {
	z.leaderMutex.Lock()
	defer z.leaderMutex.Unlock()
	if z.isLeader != isLeader {
		z.isLeader = isLeader
		if isLeader {
			z.log.Infof("has become the leader.")
		} else {
			z.log.Infof("is no longer the leader.")
		}
	}
}

// monitorConnection monitors the connection to Zookeeper and ensures that leadership is lost if the connection is disconnected.
// When reconnected, it attempts to reacquire leadership.
func (z *leaderSelector) monitorConnection() {
	for {
		select {
		case <-z.ctx.Done():
			z.log.Info("stop signal received, exiting connection monitoring loop.")
			return
		default:
			// Check if the connection is still valid
			state := z.conn.State()
			if state == zk.StateDisconnected || state == zk.StateExpired || state == zk.StateConnecting {
				z.log.Warn("disconnected from Zookeeper, losing leadership status")
				z.updateLeaderStatus(false)

				// Wait until the connection is re-established
				for state == zk.StateDisconnected {
					time.Sleep(5 * time.Second)
					state = z.conn.State()
				}

				z.log.Info("reconnected to Zookeeper, attempting to reacquire leadership")
				_, _ = z.tryElectLeader()
			}
			time.Sleep(1 * time.Second)
		}
	}
}
