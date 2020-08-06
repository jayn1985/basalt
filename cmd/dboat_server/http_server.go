package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/julienschmidt/httprouter"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/smallnest/log"
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ReqType byte

const (
	Add ReqType = iota
	AddMany
	Remove
	Drop
	Clear
	Exists
	Card
	Inter
	InterStore
	Union
	UnionStore
	Xor
	XorStore
	Diff
	DiffStore
)

type BasaltHttpServer struct {
	server *http.Server
	nh *dragonboat.NodeHost
	rs *client.Session
}

type BasaltData struct {
	Type ReqType
	Names []string  // for collection operations, use [dst, name1, name2, ...]
	Values []uint32
}

func NewServer(addr string, nh *dragonboat.NodeHost) *BasaltHttpServer {
	s := &http.Server{
		Addr: addr,
		ReadTimeout: 60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	rs := nh.GetNoOPSession(basaltClusterId)
	srv := &BasaltHttpServer{
		server: s,
		nh: nh,
		rs: rs,
	}

	srv.initRouter()
	return srv
}

func (s *BasaltHttpServer) Serve() error {
	return s.server.ListenAndServe()
}

func (s *BasaltHttpServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	s.nh.Stop()
	return s.server.Shutdown(ctx)
}

func (s *BasaltHttpServer) initRouter() {
	router := httprouter.New()

	router.POST("/add/:name/:value", s.add)
	router.POST("/addmany/:name/:values", s.addMany)
	router.POST("/remove/:name/:value", s.remove)
	router.POST("/drop/:name", s.drop)
	router.POST("/clear/:name", s.clear)
	router.GET("/exists/:name/:value", s.exists)
	router.GET("/card/:name", s.card)

	router.GET("/inter/:names", s.inter)
	router.GET("/interstore/:dst/:names", s.interStore)

	router.GET("/union/:names", s.union)
	router.GET("/unionstore/:dst/:names", s.unionStore)

	router.GET("/xor/:name1/:name2", s.xor)
	router.GET("/xorstore/:dst/:name1/:name2", s.xorStore)

	router.GET("/diff/:name1/:name2", s.diff)
	router.GET("/diffstore/:dst/:name1/:name2", s.diffStore)

	s.server.Handler = router
}

func (s *BasaltHttpServer) add(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	value := params.ByName("value")

	val, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		w.Write([]byte("INVALID DATA"))
		return
	}

	bd := &BasaltData{
		Type: Add,
		Names: []string { name },
		Values: []uint32 { uint32(val) },
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) addMany(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	values := params.ByName("values")

	var vals []uint32
	vs := strings.Split(values, ",")
	for _, val := range vs {
		v, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			w.Write([]byte("INVALID DATA"))
			return
		}

		vals = append(vals, uint32(v))
	}

	bd := &BasaltData{
		Type: AddMany,
		Names: []string { name },
		Values: vals,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) drop(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")

	bd := &BasaltData{
		Type: Drop,
		Names: []string { name },
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) clear(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")

	bd := &BasaltData{
		Type: Clear,
		Names: []string { name },
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) remove(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	value := params.ByName("value")

	val, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		w.Write([]byte("INVALID DATA"))
		return
	}

	bd := &BasaltData{
		Type: Remove,
		Names: []string { name },
		Values: []uint32 { uint32(val) },
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) exists(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	value := params.ByName("value")

	val, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		w.Write([]byte("INVALID DATA"))
		return
	}

	bd := &BasaltData{
		Type: Exists,
		Names: []string { name },
		Values: []uint32 { uint32(val) },
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strconv.FormatBool(result.(bool))))
}

func (s *BasaltHttpServer) card(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")

	bd := &BasaltData{
		Type: Card,
		Names: []string { name },
		Values: nil,
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strconv.FormatUint(result.(uint64), 10)))
}

func (s *BasaltHttpServer) inter(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	names := params.ByName("names")

	bd := &BasaltData{
		Type: Inter,
		Names: strings.Split(names, ","),
		Values: nil,
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strings.Join(strings.Fields(fmt.Sprint(result.([]uint32))), ",")))
}

func (s *BasaltHttpServer) interStore(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	names := params.ByName("names")
	dst := params.ByName("dst")

	ns := []string { dst }
	ns = append(ns, strings.Split(names, ",")...)

	bd := &BasaltData{
		Type: Inter,
		Names: ns,
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) union(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	names := params.ByName("names")

	bd := &BasaltData{
		Type: Union,
		Names: strings.Split(names, ","),
		Values: nil,
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strings.Join(strings.Fields(fmt.Sprint(result.([]uint32))), ",")))
}

func (s *BasaltHttpServer) unionStore(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	names := params.ByName("names")
	dst := params.ByName("dst")

	ns := []string { dst }
	ns = append(ns, strings.Split(names, ",")...)

	bd := &BasaltData{
		Type: UnionStore,
		Names: ns,
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) diff(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name1 := params.ByName("name1")
	name2 := params.ByName("name2")

	bd := &BasaltData{
		Type: Diff,
		Names: []string { name1, name2 },
		Values: nil,
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strings.Join(strings.Fields(fmt.Sprint(result.([]uint32))), ",")))
}

func (s *BasaltHttpServer) diffStore(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name1 := params.ByName("name1")
	name2 := params.ByName("name2")
	dst := params.ByName("dst")

	bd := &BasaltData{
		Type: DiffStore,
		Names: []string { dst, name1, name2 },
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) xor(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name1 := params.ByName("name1")
	name2 := params.ByName("name2")

	bd := &BasaltData{
		Type: Xor,
		Names: []string { name1, name2 },
		Values: nil,
	}

	result := s.doSyncRead(bd)
	if _, ok := result.(error); ok {
		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strings.Join(strings.Fields(fmt.Sprint(result.([]uint32))), ",")))
}

func (s *BasaltHttpServer) xorStore(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name1 := params.ByName("name1")
	name2 := params.ByName("name2")
	dst := params.ByName("dst")

	bd := &BasaltData{
		Type: XorStore,
		Names: []string { dst, name1, name2 },
		Values: nil,
	}

	s.doSyncPropose(bd, w)
}

func (s *BasaltHttpServer) doSyncPropose(reqData *BasaltData, w http.ResponseWriter) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	data, _ := json.Marshal(reqData)
	_, err := s.nh.SyncPropose(ctx, s.rs, data)
	if err != nil {
		log.Errorf("sync propose error: %v", err)

		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte("SUCCESS"))
}

func (s *BasaltHttpServer) doSyncRead(reqData *BasaltData) interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	data, _ := json.Marshal(reqData)
	result, err := s.nh.SyncRead(ctx, basaltClusterId, data)
	if err != nil {
		log.Errorf("sync read error: %v", err)
		return errors.New("sync read error")
	}

	return result
}


