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
	out := new(kvrpcpb.RawGetResponse)
	r, err0 := server.storage.Reader(req.Context)
	if err0 != nil {
		out.Error = err0.Error()
		out.NotFound = true
		return out, err0
	}
	value, err := r.GetCF(req.Cf, req.Key)
	if err != nil && err.Error() == "Key not found" {
		err = nil
	}
	if value == nil {
		out.NotFound = true
	} else {
		out.NotFound = false
		out.Value = value
	}
	return out, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	out := new(kvrpcpb.RawPutResponse)
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		out.Error = err.Error()
	}
	return out, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	out := new(kvrpcpb.RawDeleteResponse)
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		}}
	err := server.storage.Write(req.Context, batch)
	// if err != nil {
	// 	out.Error = err.Error()
	// }
	return out, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	out := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	// var results []*kvrpcpb.KvPair
	var index uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid() && index < req.Limit; iter.Next() {
		item := iter.Item()
		value, _ := item.Value()
		out.Kvs = append(out.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		index++
	}
	return out, err
}
