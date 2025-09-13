package fdb

import (
	"github.com/urfave/cli/v2"
)

var (
	Directory          = "etcd"
	CleanDirOnStart    = false
	LogConflictingKeys = false

	// FDB TLS
	// https://apple.github.io/foundationdb/tls.html#configuring-tls
	TLSCertificateFile = ""
	TLSKeyFile         = ""
	TLSPassword        = ""
	TLSCAFile          = ""
	TLSVerifyPeers     = ""

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
		&cli.StringFlag{
			Name:        "fdb-tls-certificate-file",
			Usage:       "Path to the file from which the local certificates can be loaded",
			Destination: &TLSCertificateFile,
		},
		&cli.StringFlag{
			Name:        "fdb-tls-key-file",
			Usage:       "Path to the file from which to load the private key",
			Destination: &TLSKeyFile,
		},
		&cli.StringFlag{
			Name:        "fdb-tls-password",
			Usage:       "The byte-string representing the passcode for unencrypting the private key",
			Destination: &TLSPassword,
		},
		&cli.StringFlag{
			Name:        "fdb-tls-ca-file",
			Usage:       "The byte-string for the verification of peer certificates and sessions",
			Destination: &TLSCAFile,
		},
		&cli.StringFlag{
			Name:        "fdb-tls-verify-peers",
			Usage:       "The byte-string for the verification of peer certificates and sessions",
			Destination: &TLSVerifyPeers,
		},
		&cli.BoolFlag{
			Name:        "fdb-log-conflicting-keys",
			Usage:       "Log conflicting keys when a transaction conflict occurs. Useful for debugging.",
			Destination: &LogConflictingKeys,
		},
	}
}
