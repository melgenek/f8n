package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	_ "unsafe" // required for go:linkname
)

//go:linkname setTransactionOption github.com/apple/fdb-operator/bindings/go/src/fdb.TransactionOptions.setOpt
func setTransactionOption(opts fdb.TransactionOptions, code int, param []byte) error
