package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/rpcxio/basalt"
	"io"
)

type BasaltStateMachine struct {
	ClusterId uint64
	NodeId uint64
	Bitmaps *basalt.Bitmaps
}

func NewBasalStateMachine(clusterId, nodeId uint64) sm.IStateMachine {
	return &BasaltStateMachine{
		ClusterId: clusterId,
		NodeId: nodeId,
		Bitmaps: basalt.NewBitmaps(),
	}
}

func CreateBasaltStateMachineHandler(bm *basalt.Bitmaps) func(uint64, uint64) sm.IStateMachine {
	bsm := &BasaltStateMachine{
		Bitmaps: bm,
	}

	return func(clusterId, nodeId uint64) sm.IStateMachine {
		bsm.ClusterId = clusterId
		bsm.NodeId = nodeId

		return bsm
	}
}

func (bsm *BasaltStateMachine) Lookup(query interface{}) (interface{}, error) {
	var reqData BasaltData
	err := json.Unmarshal(query.([]byte), &reqData)
	if err != nil {
		return nil, err
	}

	switch reqData.Type {
	case Exists:
		return bsm.Bitmaps.Exists(reqData.Names[0], reqData.Values[0]), nil
	}

	return nil, errors.New("invalid request type")
}

func (bsm *BasaltStateMachine) Update(data []byte) (sm.Result, error) {
	var reqData BasaltData
	err := json.Unmarshal(data, &reqData)
	if err != nil {
		return sm.Result{}, err
	}

	switch reqData.Type {
	case Add:
		bsm.Bitmaps.Add(reqData.Names[0], reqData.Values[0], false)
		return sm.Result{}, nil
	}

	return sm.Result{}, errors.New("invalid request type")
}

func (bsm *BasaltStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	return bsm.Bitmaps.Save(w)
}

func (bsm *BasaltStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	bm := basalt.NewBitmaps()

	if err := bm.Read(r); err != nil {
		return err
	}

	bsm.Bitmaps = bm
	return nil
}

func (bsm *BasaltStateMachine) Close() error {
	return nil
}

func (bsm *BasaltStateMachine) GetHash() (uint64, error) {
	h := md5.New()

	if err := bsm.Bitmaps.Save(h); err != nil {
		return 0, err
	}

	data := h.Sum(nil)
	return binary.LittleEndian.Uint64(data[:8]), nil
}


