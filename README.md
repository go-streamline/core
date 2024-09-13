# Streamline Core

> **Note**: This README is AI generated, so it might be incomplete or inaccurate. Please don't hesitate to open issues or ask for help.

The **Streamline Core** repository provides essential components and default implementations for the Streamline workflow engine. It includes foundational elements like the Write-Ahead Logger (WAL), Branch Tracker, Flow Manager, and more. These components are designed to be flexible and extensible, allowing developers to customize and extend functionalities as needed.

> **Note:** This project is a **Work In Progress (WIP)**. Both the core components and their tests are under active development. Contributions and feedback are welcome!

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Components](#components)
    - [Write-Ahead Logger (WAL)](#write-ahead-logger-wal)
        - [Database WAL](#database-wal)
        - [File-based WAL](#file-based-wal)
    - [Branch Tracker](#branch-tracker)
    - [Flow Manager](#flow-manager)
        - [In-Memory Flow Manager](#in-memory-flow-manager)
        - [Database Flow Manager](#database-flow-manager)
- [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Usage Examples](#usage-examples)
- [Extending the Core Components](#extending-the-core-components)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Streamline Core repository provides the building blocks for the Streamline Engine. It offers default implementations of critical components that handle workflow definitions, execution tracking, and fault tolerance mechanisms.

## Features

- **Default Implementations**: Ready-to-use components for common functionalities.
- **Extensibility**: Interfaces and base implementations that can be extended or customized.
- **Modular Design**: Components are decoupled, making it easy to swap implementations.
- **Support for Persistence**: Database-backed implementations for flow management and write-ahead logging.

## Components

### Write-Ahead Logger (WAL)

The Write-Ahead Logger is crucial for ensuring fault tolerance and recovery in case of failures. It records each processing step, allowing the engine to recover and resume from the last known state.

#### Database WAL

The **Database Write-Ahead Logger** stores log entries in a database using GORM.

```go
package db

type WriteAheadLogger struct {
	db  *gorm.DB
	log *logrus.Logger
}

func NewDBWriteAheadLogger(db *gorm.DB, log *logrus.Logger) (definitions.WriteAheadLogger, error) {
	if err := db.AutoMigrate(&walLogEntry{}); err != nil {
		return nil, err
	}

	return &WriteAheadLogger{
		db:  db,
		log: log,
	}, nil
}
```

**Explanation:**

- `WriteAheadLogger` struct holds a reference to the database and a logger.
- `NewDBWriteAheadLogger` initializes the database WAL, performing auto-migration for the `walLogEntry` model.

#### File-based WAL

The **File-based Write-Ahead Logger** writes log entries to a file using `lumberjack` for log rotation.

```go
package wal

type DefaultWriteAheadLogger struct {
	logger   *logrus.Logger
	log      *logrus.Logger
	filePath string
	enabled  bool
}

func NewWriteAheadLogger(walFilePath string, conf config.WriteAheadLogging, log *logrus.Logger) (definitions.WriteAheadLogger, error) {
	err := utils.CreateDirsIfNotExist(walFilePath, "")
	if err != nil {
		return nil, err
	}
	walLogger := logrus.New()

	walLogger.Out = &lumberjack.Logger{
		Filename:   walFilePath,
		MaxSize:    conf.MaxSizeMB,
		MaxBackups: conf.MaxBackups,
		MaxAge:     conf.MaxAgeDays,
		Compress:   true,
	}

	walLogger.SetFormatter(&logrus.JSONFormatter{})
	walLogger.SetLevel(logrus.InfoLevel)

	return &DefaultWriteAheadLogger{
		logger:   walLogger,
		filePath: walFilePath,
		enabled:  conf.Enabled,
		log:      log,
	}, nil
}
```

**Explanation:**

- `DefaultWriteAheadLogger` manages logging to a file.
- `NewWriteAheadLogger` initializes the logger with specified configurations, such as log rotation settings.

### Branch Tracker

The Branch Tracker keeps track of processor execution within a session, managing dependencies and completion states.

```go
package track

type branchTracker struct {
	mu             sync.Mutex
	pendingTasks   map[uuid.UUID]map[uuid.UUID]bool // sessionID -> processorID -> pending (true if still pending)
	completedTasks map[uuid.UUID][]uuid.UUID        // sessionID -> list of completed processor IDs
	dependencies   map[uuid.UUID][]uuid.UUID        // processorID -> list of dependent processorIDs
}

func NewBranchTracker() definitions.BranchTracker {
	return &branchTracker{
		pendingTasks:   make(map[uuid.UUID]map[uuid.UUID]bool),
		completedTasks: make(map[uuid.UUID][]uuid.UUID),
		dependencies:   make(map[uuid.UUID][]uuid.UUID),
	}
}
```

**Explanation:**

- `branchTracker` struct maintains maps to track pending and completed tasks, along with dependencies.
- `NewBranchTracker` initializes a new instance of the branch tracker.

### Flow Manager

The Flow Manager handles the storage and retrieval of flow definitions, including processors and trigger processors.

#### In-Memory Flow Manager

An in-memory implementation suitable for testing or simple use cases.

```go
package flow

type InMemoryFlowManager struct {
	flows                  map[uuid.UUID]*definitions.Flow
	flowToProcessor        map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor
	flowToTriggerProcessor map[uuid.UUID]map[uuid.UUID]*definitions.SimpleTriggerProcessor
	flowUpdates            map[uuid.UUID]time.Time
	mu                     sync.RWMutex
}

func NewInMemoryFlowManager() definitions.FlowManager {
	return &InMemoryFlowManager{
		flows:                  make(map[uuid.UUID]*definitions.Flow),
		flowToProcessor:        make(map[uuid.UUID]map[uuid.UUID]*definitions.SimpleProcessor),
		flowToTriggerProcessor: make(map[uuid.UUID]map[uuid.UUID]*definitions.SimpleTriggerProcessor),
		flowUpdates:            make(map[uuid.UUID]time.Time),
	}
}
```

**Explanation:**

- `InMemoryFlowManager` stores flows and processors in memory.
- Useful for scenarios where persistence is not required.

#### Database Flow Manager

A database-backed implementation using GORM and caching for efficient flow management.

```go
package persist

type DBFlowManager struct {
	db        *gorm.DB
	flowCache *cache.Cache[*definitions.Flow]
	ctx       context.Context
}

// NewDBFlowManager creates a new instance of DBFlowManager with an in-memory cache for flows.
func NewDBFlowManager(db *gorm.DB) (definitions.FlowManager, error) {
	err := db.AutoMigrate(&flowModel{}, &processorModel{}, &triggerProcessorModel{})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotRunMigrations, err)
	}

	ristrettoCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     100,
		BufferItems: 64,
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrFailedToInitializeCache, err)
	}

	ristrettoStore := ristrettostore.NewRistretto(ristrettoCache)
	flowCache := cache.New[*definitions.Flow](ristrettoStore)

	return &DBFlowManager{
		db:        db,
		flowCache: flowCache,
		ctx:       context.Background(),
	}, nil
}
```

**Explanation:**

- `DBFlowManager` manages flows stored in a database and uses a cache for performance.
- `NewDBFlowManager` initializes the database tables and caching mechanisms.

## Getting Started

### Installation

To install the Streamline Core components, you can use `go get`:

```bash
go get github.com/go-streamline/core
```

### Usage Examples

Here's how you can use the default implementations in your project:

**Initializing the Database Flow Manager:**

```go
import (
	"github.com/go-streamline/core/flow/persist"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// Initialize the database connection
	db, err := gorm.Open(sqlite.Open("streamline.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	// Initialize the flow manager
	flowManager, err := persist.NewDBFlowManager(db)
	if err != nil {
		log.Fatalf("failed to initialize flow manager: %v", err)
	}

	// Use the flow manager
	// ...
}
```

**Initializing the File-based Write-Ahead Logger:**

```go
import (
	"github.com/go-streamline/core/wal"
	"github.com/sirupsen/logrus"
)

func main() {
	// Initialize the logger
	logger := logrus.New()

	// Configure WAL settings
	conf := config.WriteAheadLogging{
		Enabled:     true,
		MaxSizeMB:   100,
		MaxBackups:  5,
		MaxAgeDays:  30,
	}

	// Initialize the write-ahead logger
	walFilePath := "/path/to/wal.log"
	writeAheadLogger, err := wal.NewWriteAheadLogger(walFilePath, conf, logger)
	if err != nil {
		logger.Fatalf("failed to initialize write-ahead logger: %v", err)
	}

	// Use the write-ahead logger
	// ...
}
```

**Using the Branch Tracker:**

```go
import (
	"github.com/go-streamline/core/track"
)

func main() {
	// Initialize the branch tracker
	branchTracker := track.NewBranchTracker()

	// Use the branch tracker to manage processing tasks
	// ...
}
```

## Extending the Core Components

The core components are designed to be extensible. You can implement your own versions of the interfaces provided in the `definitions` package to customize behavior.

For example, to create a custom Write-Ahead Logger:

```go
type MyCustomWriteAheadLogger struct {
	// Your fields here
}

func (w *MyCustomWriteAheadLogger) WriteEntry(entry definitions.LogEntry) error {
	// Implement your custom logic
}

func (w *MyCustomWriteAheadLogger) ReadLastEntries() ([]definitions.LogEntry, error) {
	// Implement your custom logic
}
```

Similarly, you can create custom implementations for the Flow Manager or Branch Tracker.

## Roadmap

- [ ] Complete default implementations for all core components.
- [ ] Improve documentation and usage examples.
- [ ] Add comprehensive tests for core components.
- [ ] Implement additional storage options (e.g., Redis, Cassandra).
- [ ] Enhance caching strategies and performance optimizations.

## Contributing

Contributions are welcome! Please open issues for any bugs or feature requests. When submitting pull requests, ensure that your code adheres to the existing style and includes tests where appropriate.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a pull request

## License

This project is licensed under the terms of the [MIT license](LICENSE).