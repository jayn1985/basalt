package main

import (
	"context"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/smallnest/log"
	"net/http"
	"strconv"
	"time"
)

type ReqType int

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
	//router.POST("/addmany/:name/:values", s.addMany)
	//router.POST("/remove/:name/:value", s.remove)
	//router.POST("/drop/:name", s.drop)
	//router.POST("/clear/:name", s.clear)
	router.GET("/exists/:name/:value", s.exists)
	//router.GET("/card/:name", s.card)

	//router.GET("/inter/:names", s.inter)
	//router.GET("/interstore/:dst/:names", s.interStore)

	//router.GET("/union/:names", s.union)
	//router.GET("/unionstore/:dst/:names", s.unionStore)

	//router.GET("/xor/:name1/:name2", s.xor)
	//router.GET("/xorstore/:dst/:name1/:name2", s.xorStore)

	//router.GET("/diff/:name1/:name2", s.diff)
	//router.GET("/diffstore/:dst/:name1/:name2", s.diffStore)

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	bd := &BasaltData{
		Type: Add,
		Names: []string { name },
		Values: []uint32 { uint32(val) },
	}

	data, _ := json.Marshal(bd)
	_, err = s.nh.SyncPropose(ctx, s.rs, data)
	if err != nil {
		log.Errorf("sync propose error: %v", err)

		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte("SUCCESS"))
}

func (s *BasaltHttpServer) exists(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	value := params.ByName("value")

	val, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		w.Write([]byte("INVALID DATA"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	bd := &BasaltData{
		Type: Exists,
		Names: []string { name },
		Values: []uint32 { uint32(val) },
	}

	data, _ := json.Marshal(bd)
	result, err := s.nh.SyncRead(ctx, basaltClusterId, data)
	if err != nil {
		log.Errorf("sync read error: %v", err)

		w.Write([]byte("OPERATION ERROR"))
		return
	}

	w.Write([]byte(strconv.FormatBool(result.(bool))))
}


