package main

import (
	"flag"
	"fmt"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	basaltClusterId uint64 = 100
)

var (
	port = flag.Int("port", 18419, "server port")

	peers = flag.String("peers", "localhost:63001", "dragonboat peers addresses with comma separated")
	nodeId = flag.Int("nodeid", 1, "dragonboat node id")
	join = flag.Bool("join", false, "new added node")
	dataBaseDir = flag.String("basedir", "/Users/jayn1985/basalt", "dragonboat wal & node host base dir")
)

func main() {
	flag.Parse()

	if *peers == "" {
		log.Fatal("peers can not be empty")
	}

	hosts := strings.Split(*peers, ",")
	if *nodeId < 1 || *nodeId > len(hosts) {
		log.Fatal("nodeid should be in [1, len(peer hosts)] scope")
	}

	var nodeAddr string
	members := make(map[uint64]string)
	for idx, host := range hosts {
		if idx + 1 == *nodeId {
			nodeAddr = host
		}

		members[uint64(idx + 1)] = host
	}

	rc := config.Config{
		NodeID: uint64(*nodeId),
		ClusterID: basaltClusterId,
		ElectionRTT: 10,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		SnapshotEntries: 10000,
		CompactionOverhead: 500,

	}

	dataDir := filepath.Join(*dataBaseDir, fmt.Sprintf("node-%d", *nodeId))
	nhc := config.NodeHostConfig{
		WALDir: dataDir,
		NodeHostDir: dataDir,
		RTTMillisecond: 200,
		RaftAddress: nodeAddr,
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Fatalf("failed to start dragonboat node host: %v", err)
	}

	//bitmaps := basalt.NewBitmaps()
	//hdr := CreateBasaltStateMachineHandler(bitmaps)
	if err = nh.StartCluster(members, *join, NewBasalStateMachine, rc); err != nil {
		log.Fatalf("failed to start cluster: %v\n", err)
	}

	srv := NewServer(fmt.Sprintf(":%d", *port), nh, nil)

	go func() {
		if err := srv.Serve(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start basalt server: %v", err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	srv.Close()
}
