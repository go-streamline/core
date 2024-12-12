package track

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"sync"
)

type branchTracker struct {
	mu             sync.Mutex
	pendingTasks   map[uuid.UUID]map[uuid.UUID]bool // sessionID -> processorID -> pending (true if still pending)
	completedTasks map[uuid.UUID][]uuid.UUID        // sessionID -> list of completed processor IDs
	dependencies   map[uuid.UUID][]uuid.UUID        // processorID -> list of dependent processorIDs
	log            *logrus.Logger
}

func NewBranchTracker(log *logrus.Logger) definitions.BranchTracker {
	return &branchTracker{
		pendingTasks:   make(map[uuid.UUID]map[uuid.UUID]bool),
		completedTasks: make(map[uuid.UUID][]uuid.UUID),
		dependencies:   make(map[uuid.UUID][]uuid.UUID),
		log:            log,
	}
}

func (b *branchTracker) AddProcessor(sessionID uuid.UUID, processorID uuid.UUID, nextProcessorIDs []uuid.UUID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Add the processor to pending tasks
	if _, exists := b.pendingTasks[sessionID]; !exists {
		b.pendingTasks[sessionID] = make(map[uuid.UUID]bool)
	}
	b.pendingTasks[sessionID][processorID] = true

	// Log this for debugging
	b.log.Infof("Processor %s added to session %s. Pending tasks: %v\n", processorID, sessionID, b.pendingTasks[sessionID])

	// Add the dependencies
	b.dependencies[processorID] = nextProcessorIDs
}

func (b *branchTracker) MarkComplete(sessionID uuid.UUID, processorID uuid.UUID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Mark processor as complete
	if _, exists := b.pendingTasks[sessionID]; exists {
		delete(b.pendingTasks[sessionID], processorID)

		// Log the completed processor
		b.log.Infof("Processor %s completed in session %s. Pending tasks: %v\n", processorID, sessionID, b.pendingTasks[sessionID])

		// Store the completed processor ID
		b.completedTasks[sessionID] = append(b.completedTasks[sessionID], processorID)

		// Check if there are dependencies to schedule
		for _, nextID := range b.dependencies[processorID] {
			// Add the next processor to pending tasks
			b.pendingTasks[sessionID][nextID] = true
			b.log.Infof("Next processor %s added after %s completed\n", nextID, processorID)
		}

		// If no more pending processors, the session is complete
		if len(b.pendingTasks[sessionID]) == 0 {
			delete(b.pendingTasks, sessionID)
			return true
		}
	}

	return false
}

func (b *branchTracker) IsComplete(sessionID uuid.UUID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, exists := b.pendingTasks[sessionID]
	return !exists
}

// GetCompletedProcessors returns a list of completed processor IDs for a given session
func (b *branchTracker) GetCompletedProcessors(sessionID uuid.UUID) []uuid.UUID {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.completedTasks[sessionID]
}

// RestoreState restores the branch tracker state from a list of completed processors
func (b *branchTracker) RestoreState(sessionID uuid.UUID, completedProcessorIDs []uuid.UUID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Mark each processor as completed in the pendingTasks map
	for _, processorID := range completedProcessorIDs {
		if _, exists := b.pendingTasks[sessionID]; exists {
			delete(b.pendingTasks[sessionID], processorID)
		}
	}

	// Store the completed processor IDs
	b.completedTasks[sessionID] = completedProcessorIDs
}
