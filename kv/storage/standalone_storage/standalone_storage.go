package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	//data map[string]string
	*engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := "/kv"
	raftPath := "/raft"
	kvDB := engine_util.CreateDB(conf.DBPath+kvPath, conf.Raft)
	raftDB := engine_util.CreateDB(conf.DBPath+raftPath, conf.Raft)
	engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	standAloneStorage := &StandAloneStorage{Engines: engine}
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	_ = s.Destroy()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.Kv.NewTransaction(false)
	return NewStandaloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		cf := m.Cf()
		key := m.Key()
		value := m.Value()
		wb.SetCF(cf, key, value)
	}
	_ = s.WriteKV(wb)

	return nil
}
