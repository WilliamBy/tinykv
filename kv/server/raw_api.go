package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	// not found
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	} else if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := []storage.Modify{storage.Modify{Data: storage.Put{Value: req.Value, Key: req.Key, Cf: req.Cf}}}
	if err := server.storage.Write(nil, modify); err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := []storage.Modify{storage.Modify{Data: storage.Put{Key: req.Key, Cf: req.Cf}}}
	if err := server.storage.Write(nil, modify); err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, nil
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	res := &kvrpcpb.RawScanResponse{}
	cnt := 0 //iteration number count
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		var key, val []byte
		var valErr error
		key = iter.Item().KeyCopy(nil)
		if val, valErr = iter.Item().ValueCopy(nil); valErr != nil {
			return &kvrpcpb.RawScanResponse{Error: valErr.Error()}, valErr
		}
		pair := &kvrpcpb.KvPair{Key: key, Value: val}
		res.Kvs = append(res.Kvs, pair)
		if cnt++; cnt >= int(req.Limit) {
			break
		}
	}
	return res, nil
}
