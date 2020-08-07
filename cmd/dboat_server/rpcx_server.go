package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/smallnest/log"
	"github.com/smallnest/rpcx/server"
	"time"
)

type ConfigRpcxOption func(*BasaltServer, *server.Server)

type BasaltRpcxServer struct {
	base *BasaltServer
	srv *server.Server
}

// Add adds a value in the bitmap with name.
func (s *BasaltRpcxServer) Add(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	bd := &BasaltData{
		Type: Add,
		Names: []string { req.Name },
		Values: []uint32 { uint32(req.Value) },
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// AddMany adds multiple values in the bitmap with name.
func (s *BasaltRpcxServer) AddMany(ctx context.Context, req *BitmapValuesRequest, reply *bool) error {
	bd := &BasaltData{
		Type: AddMany,
		Names: []string { req.Name },
		Values: req.Values,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// Remove removes a value in the bitmap with name.
func (s *BasaltRpcxServer) Remove(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	bd := &BasaltData{
		Type: Remove,
		Names: []string { req.Name },
		Values: []uint32 { uint32(req.Value) },
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// RemoveBitmap removes the bitmap.
func (s *BasaltRpcxServer) RemoveBitmap(ctx context.Context, name string, reply *bool) error {
	bd := &BasaltData{
		Type: Drop,
		Names: []string { name },
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// ClearBitmap clears the bitmap and set it to be empty.
func (s *BasaltRpcxServer) ClearBitmap(ctx context.Context, name string, reply *bool) error {
	bd := &BasaltData{
		Type: Clear,
		Names: []string { name },
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// Exists checks whether the value exists.
func (s *BasaltRpcxServer) Exists(ctx context.Context, req *BitmapValueRequest, reply *bool) error {
	bd := &BasaltData{
		Type: Exists,
		Names: []string { req.Name },
		Values: []uint32 { uint32(req.Value) },
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = false
		return nil
	}

	*reply = result.(bool)
	return nil
}

// Card gets number of integers in the bitmap.
func (s *BasaltRpcxServer) Card(ctx context.Context, name string, reply *uint64) error {
	bd := &BasaltData{
		Type: Card,
		Names: []string { name },
		Values: nil,
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = 0
		return nil
	}

	*reply = result.(uint64)
	return nil
}

// Inter gets the intersection of bitmaps.
func (s *BasaltRpcxServer) Inter(ctx context.Context, names []string, reply *[]uint32) error {
	bd := &BasaltData{
		Type: Inter,
		Names: names,
		Values: nil,
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = nil
		return nil
	}

	*reply = result.([]uint32)
	return nil
}

// InterStore gets the intersection of bitmaps and stores into destination.
func (s *BasaltRpcxServer) InterStore(ctx context.Context, req *BitmapStoreRequest, reply *bool) error {
	ns := []string { req.Destination }
	ns = append(ns, req.Names...)

	bd := &BasaltData{
		Type: InterStore,
		Names: ns,
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// Union gets the union of bitmaps.
func (s *BasaltRpcxServer) Union(ctx context.Context, names []string, reply *[]uint32) error {
	bd := &BasaltData{
		Type: Union,
		Names: names,
		Values: nil,
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = nil
		return nil
	}

	*reply = result.([]uint32)
	return nil
}

// UnionStore gets the union of bitmaps and stores into destination.
func (s *BasaltRpcxServer) UnionStore(ctx context.Context, req *BitmapStoreRequest, reply *bool) error {
	ns := []string { req.Destination }
	ns = append(ns, req.Names...)

	bd := &BasaltData{
		Type: UnionStore,
		Names: ns,
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// Xor gets the symmetric difference between bitmaps.
func (s *BasaltRpcxServer) Xor(ctx context.Context, names *BitmapPairRequest, reply *[]uint32) error {
	bd := &BasaltData{
		Type: Xor,
		Names: []string { names.Name1, names.Name2 },
		Values: nil,
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = nil
		return nil
	}

	*reply = result.([]uint32)
	return nil
}

// XorStore gets the symmetric difference between bitmaps and stores into destination.
func (s *BasaltRpcxServer) XorStore(ctx context.Context, names *BitmapDstAndPairRequest, reply *bool) error {
	bd := &BasaltData{
		Type: XorStore,
		Names: []string { names.Destination, names.Name1, names.Name2 },
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

// Diff gets the difference between two bitmaps.
func (s *BasaltRpcxServer) Diff(ctx context.Context, names *BitmapPairRequest, reply *[]uint32) error {
	bd := &BasaltData{
		Type: Diff,
		Names: []string { names.Name1, names.Name2 },
		Values: nil,
	}

	result := s.doSyncRead1(bd)
	if _, ok := result.(error); ok {
		*reply = nil
		return nil
	}

	*reply = result.([]uint32)
	return nil
}

// DiffStore gets the difference between two bitmaps and stores into destination.
func (s *BasaltRpcxServer) DiffStore(ctx context.Context, names *BitmapDstAndPairRequest, reply *bool) error {
	bd := &BasaltData{
		Type: DiffStore,
		Names: []string { names.Destination, names.Name1, names.Name2 },
		Values: nil,
	}

	err := s.doSyncPropose1(bd)

	*reply = true
	if err != nil {
		*reply = false
	}

	return nil
}

func (s *BasaltRpcxServer) doSyncPropose1(reqData *BasaltData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	data, _ := json.Marshal(reqData)
	_, err := s.base.nh.SyncPropose(ctx, s.base.rs, data)
	if err != nil {
		log.Errorf("sync propose error: %v", err)
	}

	return err
}

func (s *BasaltRpcxServer) doSyncRead1(reqData *BasaltData) interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	data, _ := json.Marshal(reqData)
	result, err := s.base.nh.SyncRead(ctx, basaltClusterId, data)
	if err != nil {
		log.Errorf("sync read error: %v", err)
		return errors.New("sync read error")
	}

	return result
}
