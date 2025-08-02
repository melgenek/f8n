package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

var one = []byte{1}

type WatchSubspace struct {
	subspace subspace.Subspace
}

func CreateWatchSubspace(directory directory.DirectorySubspace) *WatchSubspace {
	return &WatchSubspace{subspace: directory.Sub("watch")}
}

func (s *WatchSubspace) Write(tr *fdb.Transaction) error {
	tr.Add(s.subspace, one)
	return nil
}

func (s *WatchSubspace) Watch(tr *fdb.Transaction) fdb.FutureNil {
	return tr.Watch(s.subspace)
}
