package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var UseSequentialId = false

type RevisionSubspace struct {
	subspace subspace.Subspace
}

func CreateRevisionSubspace(directory directory.DirectorySubspace) *RevisionSubspace {
	if UseSequentialId {
		return &RevisionSubspace{subspace: directory.Sub("latestRev")}
	} else {
		return &RevisionSubspace{}
	}
}

func (s *RevisionSubspace) IncrementAndGet(tr *fdb.Transaction) (tuple.Versionstamp, fdb.FutureKey, error) {
	if UseSequentialId {
		value, err := tr.Get(s.subspace).Get()
		if err != nil {
			return int64ToVersionstamp(-1), nil, err
		}
		id := int64(1)
		if value != nil {
			if t, err := tuple.Unpack(value); err != nil {
				return int64ToVersionstamp(-1), nil, err
			} else {
				id = t[0].(int64)
			}
		}
		id++
		tr.Set(s.subspace, tuple.Tuple{id}.Pack())
		versionstamp := int64ToVersionstamp(id)
		return versionstamp, ConstKeyFuture{versionstamp}, nil
	} else {
		return tuple.IncompleteVersionstamp(0), tr.GetVersionstamp(), nil
	}
}

func (s *RevisionSubspace) GetLatestRev(tr *fdb.Transaction) (fdb.FutureInt64, error) {
	if UseSequentialId {
		return s.lastSequentialId(tr)
	} else {
		return tr.GetReadVersion(), nil
	}
}

func (s *RevisionSubspace) lastSequentialId(tr *fdb.Transaction) (fdb.FutureInt64, error) {
	value, err := tr.Get(s.subspace).Get()
	if err != nil {
		return ConstInt64Future{-1}, err
	}
	if value == nil {
		return ConstInt64Future{1}, nil
	}
	t, err := tuple.Unpack(value)
	if err != nil {
		return ConstInt64Future{-1}, err
	}

	return ConstInt64Future{t[0].(int64)}, nil
}

type PackKey = func(t tuple.Tuple) (fdb.Key, error)
type SetValue = func(fdb.KeyConvertible, []byte)

func GetWriteOps(tr *fdb.Transaction, subspace subspace.Subspace) (PackKey, SetValue) {
	if UseSequentialId {
		return func(t tuple.Tuple) (fdb.Key, error) {
			return subspace.Pack(t), nil
		}, tr.Set
	} else {
		return subspace.PackWithVersionstamp, tr.SetVersionstampedKey
	}
}
