package fdb

import (
	"github.com/urfave/cli/v2"
)

var (
	Directory          = "etcd"
	CleanDirOnStart    = false
	LogConflictingKeys = false

	// For testing only
	UseSequentialId = false
	APITest         = false
)

func ConfigFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "dir",
			Value:       "etcd",
			Usage:       "FoundationDB directory name where data is stored. Default is 'etcd'.",
			Destination: &Directory,
		},
		&cli.BoolFlag{
			Name:        "clean-dir-on-start",
			Usage:       "Clean the directory on start. Useful for testing.",
			Destination: &CleanDirOnStart,
		},
		&cli.BoolFlag{
			Name:        "log-conflicting-keys",
			Usage:       "Log conflicting keys when a transaction conflict occurs.",
			Destination: &LogConflictingKeys,
		},
	}
}
