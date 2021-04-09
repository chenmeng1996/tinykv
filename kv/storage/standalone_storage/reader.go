package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneReader struct {
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *StandaloneReader {
	return &StandaloneReader{
		txn: txn,
	}
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return NewStandaloneIterator(engine_util.NewCFIterator(cf, r.txn))
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}

// StandaloneIterator wraps a db iterator and only allow it to iterate in the region. It behaves as if underlying
// db only contains one region.
type StandaloneIterator struct {
	iter *engine_util.BadgerIterator
}

func NewStandaloneIterator(iter *engine_util.BadgerIterator) *StandaloneIterator {
	return &StandaloneIterator{
		iter: iter,
	}
}

func (it *StandaloneIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *StandaloneIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	return true
}

func (it *StandaloneIterator) ValidForPrefix(prefix []byte) bool {
	if !it.iter.ValidForPrefix(prefix) {
		return false
	}
	return true
}

func (it *StandaloneIterator) Close() {
	it.iter.Close()
}

func (it *StandaloneIterator) Next() {
	it.iter.Next()
}

func (it *StandaloneIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *StandaloneIterator) Rewind() {
	it.iter.Rewind()
}
