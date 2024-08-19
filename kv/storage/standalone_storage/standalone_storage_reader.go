package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

/*
*

GetCF(cf string, key []byte) ([]byte, error)
IterCF(cf string) engine_util.DBIterator
Close()
*/
type StandaloneReader struct {
	txn    *badger.Txn
	engine *engine_util.Engines
}

func NewStandaloneReader(txn *badger.Txn, engine *engine_util.Engines) *StandaloneReader {
	return &StandaloneReader{
		txn:    txn,
		engine: engine,
	}
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	bys, err := engine_util.GetCF(r.engine.Kv, cf, key)
	if err != nil && err.Error() == "Key not found" {
		err = nil
	}
	return bys, err
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}

func (r *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
