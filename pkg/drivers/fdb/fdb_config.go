package fdb

import (
	"github.com/urfave/cli/v2"
	"time"
)

var (
	Directory          = "etcd"
	CleanDirOnStart    = false
	LogConflictingKeys = false
	WriteBatchDuration = 2 * time.Millisecond

	// For testing only
	APITest = false
)

func ConfigFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "fdb-directory",
			Value:       "etcd",
			Usage:       "FoundationDB directory name where data is stored. Default is 'etcd'.",
			Destination: &Directory,
		},
		&cli.BoolFlag{
			Name:        "fdb-clean-directory-on-start",
			Usage:       "Clean the directory on start. Useful for testing.",
			Destination: &CleanDirOnStart,
		},
		&cli.BoolFlag{
			Name:        "fdb-log-conflicting-keys",
			Usage:       "Log conflicting keys when a transaction conflict occurs. Useful for debugging.",
			Destination: &LogConflictingKeys,
		},
		&cli.DurationFlag{
			Name:        "fdb-write-batch-duration",
			Value:       2 * time.Millisecond,
			Usage:       "Duration that defines how long to collect write requests in a single transaction.",
			Destination: &WriteBatchDuration,
		},
	}
}
