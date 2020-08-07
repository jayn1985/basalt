package main

import (
	"context"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/soheilhy/cmux"
	"io"
	"net"
	"net/http"
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

type BasaltData struct {
	Type ReqType
	Names []string  // for collection operations, use [dst, name1, name2, ...]
	Values []uint32
}

type BasaltServer struct {
	addr string
	nh *dragonboat.NodeHost
	rs *client.Session
	rpcxOpts []ConfigRpcxOption

	httpSrv *BasaltHttpServer
	rpcxSrv *BasaltRpcxServer
}

func NewServer(addr string, nh *dragonboat.NodeHost, rpcxOptions []ConfigRpcxOption) *BasaltServer {
	rs := nh.GetNoOPSession(basaltClusterId)

	return &BasaltServer{
		addr: addr,
		nh: nh,
		rs: rs,
		rpcxOpts: rpcxOptions,
	}
}

func (s *BasaltServer) Serve() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	m := cmux.New(ln)

	// rpcx
	rln := m.Match(rpcxPrefixByteMatcher())

	// http
	hln := m.Match(cmux.HTTP1Fast())

	go s.startRpcxServer(rln)
	go s.startHttpServer(hln)

	return m.Serve()
}

func (s *BasaltServer) startRpcxServer(ln net.Listener) error {
	srv := server.NewServer()

	for _, opt := range s.rpcxOpts {
		opt(s, srv)
	}

	brs := &BasaltRpcxServer{
		base: s,
		srv: srv,
	}

	srv.RegisterName("Bitmap", brs, "")
	s.rpcxSrv = brs

	return srv.ServeListener("tcp", ln)
}

func (s *BasaltServer) startHttpServer(ln net.Listener) error {
	srv := &http.Server{
		ReadTimeout: 60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	bhs := &BasaltHttpServer{
		base: s,
		srv: srv,
	}

	bhs.initRouter()
	s.httpSrv = bhs

	return srv.Serve(ln)
}

func (s *BasaltServer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	s.nh.Stop()
	s.httpSrv.srv.Shutdown(ctx)
	s.rpcxSrv.srv.Close()
}

func rpcxPrefixByteMatcher() cmux.Matcher {
	magic := protocol.MagicNumber()

	return func(r io.Reader) bool {
		buf := make([]byte, 1)
		n, _ := r.Read(buf)

		return n == 1 && buf[0] == magic
	}
}
