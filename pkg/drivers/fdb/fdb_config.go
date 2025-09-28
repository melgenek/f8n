package fdb

import (
	"github.com/urfave/cli/v2"
)

var (
	Directory          = "etcd"
	CleanDirOnStart    = false
	LogConflictingKeys = false
	// https://github.com/kubernetes/kubernetes/blob/master/cluster/images/etcd/migrate/options.go#L49C27-L50
	EtcdDataPrefix = "/registry"

	// For testing only
	UseSequentialId = false
	APITest         = false
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
		&cli.StringFlag{
			Name:        "etcd-data-prefix",
			Usage:       "The value of the 'etcd-data-prefix' or env var 'ETCD_DATA_PREFIX' in the API server. Default: /registry",
			Destination: &EtcdDataPrefix,
			EnvVars:     []string{"ETCD_DATA_PREFIX"},
		},
	}
}
