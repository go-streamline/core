package config

type WriteAheadLogging struct {
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
	Enabled    bool
}
