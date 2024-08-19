package standalone_storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	cfg    *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{cfg: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.cfg.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	err := os.MkdirAll(kvPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	kvDB := engine_util.CreateDB(s.cfg.DBPath, false)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, raftPath)
	s.engine = engines
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engine.Destroy(); err != nil {
		panic(err)
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandaloneReader(txn, s.engine), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, item := range batch {
		wb.SetCF(item.Cf(), item.Key(), item.Value())
	}
	err := wb.WriteToDB(s.engine.Kv)
	if err != nil {
		return err
	}
	return nil
}

func Join(prefix string, key []byte) string {
	return fmt.Sprintf(prefix+"%s", key)
}
