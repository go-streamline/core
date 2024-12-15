package zookeeper

import (
	"context"
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

// coordinator is an implementation of the Coordinator interface using Zookeeper for leader election.
// It assigns leaders for each trigger processor using round-robin so all nodes get an equal chance to be the leader.
// It also monitors the nodes in the cluster and triggers rebalancing when nodes change.
// If current node is the coordinator, it assigns a leader for the trigger processor and updates the znode.
// If current node is not the coordinator, it watches the znode for the trigger processor to check if it is the leader.
type coordinator struct {
	leaderSelector definitions.LeaderSelector    // LeaderSelector to determine if the current node is the coordinator
	conn           coordinatorZookeeperInterface // Zookeeper connection
	tpLeaderPath   string                        // Path where the leader nodes for each TP are stored
	tpLeaders      map[uuid.UUID]string          // Map of trigger processors and their assigned leaders (tpID -> leaderNode)
	roundRobinIdx  int                           // Index for round-robin leader selection
	mu             sync.Mutex                    // Mutex to protect tpLeaders
	log            *logrus.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	nodes          []string // Cached list of nodes in the cluster
}

type coordinatorZookeeperInterface interface {
	zkCreateFullPathInterface
	Get(path string) ([]byte, *zk.Stat, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
}

// NewCoordinator creates a new Coordinator instance.
func NewCoordinator(
	leaderSelector definitions.LeaderSelector,
	conn coordinatorZookeeperInterface,
	tpLeaderPath string,
	logFactory definitions.LoggerFactory,
) definitions.Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	tpLeaderPath = path.Dir(tpLeaderPath)
	c := &coordinator{
		leaderSelector: leaderSelector,
		conn:           conn,
		tpLeaderPath:   tpLeaderPath,
		tpLeaders:      make(map[uuid.UUID]string),
		log:            logFactory.GetLogger(fmt.Sprintf("coordinator-%d", xxhash.Sum64String(tpLeaderPath))),
		ctx:            ctx,
		cancel:         cancel,
		nodes:          []string{},
	}

	// Start monitoring nodes in the cluster
	go c.monitorNodeChanges()

	return c
}

// IsLeader checks if the current node is the leader for the given trigger processor (tpID).
// If the node is the coordinator, it assigns a leader and updates the znode for the TP.
// If the node is not the coordinator, it monitors the znode for the TP to check if it is the leader.
func (c *coordinator) IsLeader(tpID uuid.UUID) (bool, error) {
	isCoordinator, err := c.leaderSelector.IsLeader()
	if err != nil {
		return false, err
	}

	if isCoordinator {
		// This node is the coordinator, assign a leader for the TP if not already assigned
		return c.getOrAssignLeader(tpID)
	}

	// This node is not the coordinator, watch the znode for changes
	return c.watchZNode(tpID)
}

// getOrAssignLeader retrieves or assigns a leader for the trigger processor (tpID).
// It updates the znode for the TP with the chosen leader if not already assigned.
func (c *coordinator) getOrAssignLeader(tpID uuid.UUID) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the leader is already assigned for this TP
	if leader, exists := c.tpLeaders[tpID]; exists {
		return leader == c.leaderSelector.ParticipantName(), nil
	}

	leaderNode, err := c.assignNewLeaderNode(tpID)

	if err != nil {
		return false, err
	}

	// Check if the current node is the leader
	return leaderNode == c.leaderSelector.ParticipantName(), nil
}

func (c *coordinator) assignNewLeaderNode(tpID uuid.UUID) (string, error) {
	if (len(c.nodes)) == 0 {
		nodes, err := c.leaderSelector.Participants()
		if err != nil {
			return "", err
		}
		if len(nodes) == 0 {
			return "", errors.New("no nodes available for leader selection")
		}
		c.nodes = nodes
	}

	// Round-robin leader selection
	leaderNode := c.nodes[c.roundRobinIdx]
	c.roundRobinIdx = (c.roundRobinIdx + 1) % len(c.nodes)

	// Update the tpLeaders map and the Zookeeper znode for this TP
	c.tpLeaders[tpID] = leaderNode
	err := c.updateZNode(tpID, leaderNode)

	return leaderNode, err
}

// watchZNode watches the znode for the given trigger processor (tpID).
// It checks if the current node is the leader by monitoring the znode data.
func (c *coordinator) watchZNode(tpID uuid.UUID) (bool, error) {
	// Watch the znode for changes
	path := fmt.Sprintf("%s/%s", c.tpLeaderPath, tpID)
	data, _, err := c.conn.Get(path)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			c.log.Warnf("znode for tpID %s does not exist, retrying election...", tpID)
			return false, nil
		}
		return false, err
	}

	leaderNode := string(data)
	if leaderNode == c.leaderSelector.ParticipantName() {
		// This node is the leader for the TP
		return true, nil
	}

	return false, nil
}

// monitorNodeChanges uses the leaderSelector to subscribe to node change events and trigger rebalancing when necessary.
func (c *coordinator) monitorNodeChanges() {
	nodeChangeCh := c.leaderSelector.ParticipantsChangeChannel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-nodeChangeCh:
			c.mu.Lock()
			nodes, err := c.leaderSelector.Participants()
			if err != nil {
				c.log.WithError(err).Error("failed to retrieve nodes for monitoring after event")
				c.mu.Unlock()
				continue
			}

			if !equalNodeLists(nodes, c.nodes) {
				c.log.Infof("nodes changed, triggering rebalancing... old: %v, new: %v", c.nodes, nodes)
				c.nodes = nodes
				// Trigger rebalancing if nodes change
				c.mu.Unlock()
				c.rebalanceLeadership()
			} else {
				c.mu.Unlock()
			}
		}
	}
}

// rebalanceLeadership reassigns trigger processor leadership when nodes change.
func (c *coordinator) rebalanceLeadership() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		// Check if the current node is still the coordinator
		isLeader, err := c.leaderSelector.IsLeader()
		if err != nil {
			c.log.WithError(err).Error("failed to check leader status during rebalancing")
			time.Sleep(5 * time.Second) // Backoff before retrying
			continue
		}

		if !isLeader {
			c.log.Info("node is no longer the leader, stopping rebalancing")
			return // Exit the loop if the node is no longer the coordinator
		}

		// Perform the rebalancing if still the leader
		err = c.attemptRebalance()
		if err != nil {
			c.log.WithError(err).Error("rebalancing failed, retrying...")
			time.Sleep(5 * time.Second) // Backoff before retrying
			continue
		}

		// If rebalancing is successful, break out of the loop
		break
	}
}

// attemptRebalance performs the actual rebalancing logic
func (c *coordinator) attemptRebalance() error {
	nodes := c.nodes
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for rebalancing")
	}

	// Reassign leaders in round-robin fashion based on updated node list
	for tpID := range c.tpLeaders {
		_, err := c.assignNewLeaderNode(tpID)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateZNode updates the leader znode for a trigger processor
func (c *coordinator) updateZNode(tpID uuid.UUID, leaderNode string) error {
	znode := fmt.Sprintf("%s/%s", c.tpLeaderPath, tpID)

	// check if exists first
	exists, _, err := c.conn.Exists(znode)
	if err != nil {
		return err
	}
	if !exists {
		_, _, err = CreateFullPath(c.conn, znode, []byte(leaderNode), zk.FlagEphemeral)
		if err != nil {
			return err
		}
	} else {
		_, err = c.conn.Set(znode, []byte(leaderNode), -1)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close stops the coordinator by canceling the context and stopping any leader selection.
func (c *coordinator) Close() error {
	c.cancel()
	return nil
}

// equalNodeLists checks if two lists of nodes are equal
func equalNodeLists(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !slices.Contains(a, b[i]) {
			return false
		}
	}

	return true
}
