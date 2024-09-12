package track

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newBranchTracker() *branchTracker {
	return &branchTracker{
		pendingTasks:   make(map[uuid.UUID]map[uuid.UUID]bool),
		completedTasks: make(map[uuid.UUID][]uuid.UUID),
		dependencies:   make(map[uuid.UUID][]uuid.UUID),
	}
}

func TestBranchTracker_FlowExecution_SequentialOrder(t *testing.T) {
	// finish order: p1, p2, p3, p4, p5, p6, p7, p8
	tracker := newBranchTracker()

	// Simulate session ID
	sessionID := uuid.New()

	// Flow: p1 -> [p2 -> p3 -> p4, p5 -> p6, p7] -> p8
	// Add the processors and their dependencies to the branch tracker
	p1 := uuid.New()
	p2 := uuid.New()
	p3 := uuid.New()
	p4 := uuid.New()
	p5 := uuid.New()
	p6 := uuid.New()
	p7 := uuid.New()
	p8 := uuid.New()

	// Simulate the flow by adding the processors
	// p1 has three dependencies: p2, p5, p7
	tracker.AddProcessor(sessionID, p1, []uuid.UUID{p2, p5, p7})

	// Initially, only p1 is pending execution
	assert.False(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 1, len(tracker.pendingTasks[sessionID]))
	assert.True(t, tracker.pendingTasks[sessionID][p1])

	// p1 finished, running p2, p5, p7 in parallel
	allComplete := tracker.MarkComplete(sessionID, p1)
	assert.False(t, allComplete)
	fmt.Println("Pending tasks after p1 completion:", tracker.pendingTasks[sessionID])
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p2, p5, p7 added
	assert.True(t, tracker.pendingTasks[sessionID][p2])
	assert.True(t, tracker.pendingTasks[sessionID][p5])
	assert.True(t, tracker.pendingTasks[sessionID][p7])

	// p2 finished, running p2 -> p3
	tracker.AddProcessor(sessionID, p2, []uuid.UUID{p3})
	allComplete = tracker.MarkComplete(sessionID, p2)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p3 added
	assert.True(t, tracker.pendingTasks[sessionID][p3])

	// p3 finished, running p3 -> p4
	tracker.AddProcessor(sessionID, p3, []uuid.UUID{p4})
	allComplete = tracker.MarkComplete(sessionID, p3)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p4 added
	assert.True(t, tracker.pendingTasks[sessionID][p4])

	// p4 finished, no new processor is added yet
	allComplete = tracker.MarkComplete(sessionID, p4)
	assert.False(t, allComplete)
	assert.Equal(t, 2, len(tracker.pendingTasks[sessionID])) // still p5, p7

	// p5 finished, running p5 -> p6
	tracker.AddProcessor(sessionID, p5, []uuid.UUID{p6})
	allComplete = tracker.MarkComplete(sessionID, p5)
	assert.False(t, allComplete)
	assert.Equal(t, 2, len(tracker.pendingTasks[sessionID])) // p6 added
	assert.True(t, tracker.pendingTasks[sessionID][p6])

	// p6 finished, no new processor is added
	allComplete = tracker.MarkComplete(sessionID, p6)
	assert.False(t, allComplete)
	assert.Equal(t, 1, len(tracker.pendingTasks[sessionID])) // still p7

	// p7 finished, the last parallel branch finishes
	allComplete = tracker.MarkComplete(sessionID, p7)
	assert.True(t, allComplete) // All branches are done

	// p8 should be the only task left after all branches complete
	tracker.AddProcessor(sessionID, p8, nil) // p8 is the final processor
	allComplete = tracker.MarkComplete(sessionID, p8)
	assert.True(t, allComplete) // now the flow is complete

	// Assert the flow is complete and there are no pending tasks
	assert.True(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 0, len(tracker.pendingTasks))
}

func TestBranchTracker_FlowExecution_CustomOrder1(t *testing.T) {
	// finish order: p1, p2, p5, p7, p3, p4, p6, p8
	tracker := newBranchTracker()

	// Simulate session ID
	sessionID := uuid.New()

	// Flow: p1 -> [p2 -> p3 -> p4, p5 -> p6, p7] -> p8
	// Add the processors and their dependencies to the branch tracker
	p1 := uuid.New()
	p2 := uuid.New()
	p3 := uuid.New()
	p4 := uuid.New()
	p5 := uuid.New()
	p6 := uuid.New()
	p7 := uuid.New()
	p8 := uuid.New()

	// Simulate the flow by adding the processors
	// p1 has three dependencies: p2, p5, p7
	tracker.AddProcessor(sessionID, p1, []uuid.UUID{p2, p5, p7})

	// Initially, only p1 is pending execution
	assert.False(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 1, len(tracker.pendingTasks[sessionID]))
	assert.True(t, tracker.pendingTasks[sessionID][p1])

	// p1 finished, running p2, p5, p7 in parallel
	allComplete := tracker.MarkComplete(sessionID, p1)
	assert.False(t, allComplete)
	fmt.Println("Pending tasks after p1 completion:", tracker.pendingTasks[sessionID])
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p2, p5, p7 added
	assert.True(t, tracker.pendingTasks[sessionID][p2])
	assert.True(t, tracker.pendingTasks[sessionID][p5])
	assert.True(t, tracker.pendingTasks[sessionID][p7])

	// p2 finished, running p2 -> p3
	tracker.AddProcessor(sessionID, p2, []uuid.UUID{p3})
	allComplete = tracker.MarkComplete(sessionID, p2)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p3 added
	assert.True(t, tracker.pendingTasks[sessionID][p3])

	// p3 finished, running p3 -> p4
	tracker.AddProcessor(sessionID, p3, []uuid.UUID{p4})
	allComplete = tracker.MarkComplete(sessionID, p3)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p4 added
	assert.True(t, tracker.pendingTasks[sessionID][p4])

	// p5 finished, running p5 -> p6
	tracker.AddProcessor(sessionID, p5, []uuid.UUID{p6})
	allComplete = tracker.MarkComplete(sessionID, p5)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p6 added
	assert.True(t, tracker.pendingTasks[sessionID][p6])

	// p7 finished, no new processor is added for p7
	allComplete = tracker.MarkComplete(sessionID, p7)
	assert.False(t, allComplete)
	assert.Equal(t, 2, len(tracker.pendingTasks[sessionID])) // still p4, p6

	// p4 finished, the first branch [p2 -> p3 -> p4] is done
	allComplete = tracker.MarkComplete(sessionID, p4)
	assert.False(t, allComplete)

	// p6 finished, the second branch [p5 -> p6] is done
	allComplete = tracker.MarkComplete(sessionID, p6)
	assert.True(t, allComplete)

	// p8 should be the only task left after all branches complete
	tracker.AddProcessor(sessionID, p8, nil) // p8 is the final processor
	allComplete = tracker.MarkComplete(sessionID, p8)
	assert.True(t, allComplete) // now the flow is complete

	// Assert the flow is complete and there are no pending tasks
	assert.True(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 0, len(tracker.pendingTasks))
}

func TestBranchTracker_FlowExecution_CustomOrder2(t *testing.T) {
	// finish order: p1, p5, p2, p3, p6, p7, p4, p8
	tracker := newBranchTracker()

	// Simulate session ID
	sessionID := uuid.New()

	// Flow: p1 -> [p2 -> p3 -> p4, p5 -> p6, p7] -> p8
	// Add the processors and their dependencies to the branch tracker
	p1 := uuid.New()
	p2 := uuid.New()
	p3 := uuid.New()
	p4 := uuid.New()
	p5 := uuid.New()
	p6 := uuid.New()
	p7 := uuid.New()
	p8 := uuid.New()

	// Simulate the flow by adding the processors
	// p1 has three dependencies: p2, p5, p7
	tracker.AddProcessor(sessionID, p1, []uuid.UUID{p2, p5, p7})

	// Initially, only p1 is pending execution
	assert.False(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 1, len(tracker.pendingTasks[sessionID]))
	assert.True(t, tracker.pendingTasks[sessionID][p1])

	// p1 finished, running p2, p5, p7 in parallel
	allComplete := tracker.MarkComplete(sessionID, p1)
	assert.False(t, allComplete)
	fmt.Println("Pending tasks after p1 completion:", tracker.pendingTasks[sessionID])
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p2, p5, p7 added
	assert.True(t, tracker.pendingTasks[sessionID][p2])
	assert.True(t, tracker.pendingTasks[sessionID][p5])
	assert.True(t, tracker.pendingTasks[sessionID][p7])

	// p5 finished, running p5 -> p6
	tracker.AddProcessor(sessionID, p5, []uuid.UUID{p6})
	allComplete = tracker.MarkComplete(sessionID, p5)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p6 added
	assert.True(t, tracker.pendingTasks[sessionID][p6])

	// p2 finished, running p2 -> p3
	tracker.AddProcessor(sessionID, p2, []uuid.UUID{p3})
	allComplete = tracker.MarkComplete(sessionID, p2)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p3 added
	assert.True(t, tracker.pendingTasks[sessionID][p3])

	// p3 finished, running p3 -> p4
	tracker.AddProcessor(sessionID, p3, []uuid.UUID{p4})
	allComplete = tracker.MarkComplete(sessionID, p3)
	assert.False(t, allComplete)
	assert.Equal(t, 3, len(tracker.pendingTasks[sessionID])) // p4 added
	assert.True(t, tracker.pendingTasks[sessionID][p4])

	// p6 finished, no new processor is added for p6
	allComplete = tracker.MarkComplete(sessionID, p6)
	assert.False(t, allComplete)
	assert.Equal(t, 2, len(tracker.pendingTasks[sessionID])) // still p4, p7

	// p7 finished, no new processor is added for p7
	allComplete = tracker.MarkComplete(sessionID, p7)
	assert.False(t, allComplete)
	assert.Equal(t, 1, len(tracker.pendingTasks[sessionID])) // still p4

	// p4 finished, the first branch [p2 -> p3 -> p4] is done
	allComplete = tracker.MarkComplete(sessionID, p4)
	assert.True(t, allComplete) // All branches are done

	// p8 should be the only task left after all branches complete
	tracker.AddProcessor(sessionID, p8, nil) // p8 is the final processor
	allComplete = tracker.MarkComplete(sessionID, p8)
	assert.True(t, allComplete) // now the flow is complete

	// Assert the flow is complete and there are no pending tasks
	assert.True(t, tracker.IsComplete(sessionID))
	assert.Equal(t, 0, len(tracker.pendingTasks))
}
