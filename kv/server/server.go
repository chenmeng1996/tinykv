package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil && err == nil {
		res.NotFound = true
		return res, err
	}
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	res.Value = value

	return res, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawPutResponse{}

	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}

	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	return res, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawDeleteResponse{}

	modify := storage.Modify{
		Data: storage.Delete{
			Cf:  req.GetCf(),
			Key: req.GetKey(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	return res, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	dbIterator := reader.IterCF(req.GetCf())
	defer dbIterator.Close()
	kvs := make([]*kvrpcpb.KvPair, 0)
	var limitCount uint32 = 0

	if req.StartKey != nil {
		dbIterator.Seek(req.StartKey)
	}
	for ; dbIterator.Valid(); dbIterator.Next() {
		dbItem := dbIterator.Item()
		kvPair := &kvrpcpb.KvPair{
			Key: dbItem.Key(),
		}
		v, err := dbItem.Value()
		if err != nil {
			kvPair.Error = &kvrpcpb.KeyError{}
		} else {
			kvPair.Value = v
		}
		limitCount += 1
		if limitCount > req.Limit {
			break
		}
		kvs = append(kvs, kvPair)
	}

	res.Kvs = kvs
	return res, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
