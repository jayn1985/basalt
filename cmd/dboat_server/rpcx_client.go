package main

import (
	"context"
	"flag"
	"github.com/smallnest/rpcx/client"
	"log"
)

var (
	addr = flag.String("addr", "127.0.0.1:38419", "the listened address")
)

func main() {
	flag.Parse()

	d := client.NewPeer2PeerDiscovery("tcp@" + *addr, "")
	xclient := client.NewXClient("Bitmap", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer xclient.Close()

	var ok bool
	xclient.Call(context.Background(), "Add", &BitmapValueRequest{"test2", 1}, &ok)
	xclient.Call(context.Background(), "AddMany", &BitmapValuesRequest{"test2", []uint32{2, 3, 10, 11}}, &ok)

	xclient.Call(context.Background(), "Add", &BitmapValueRequest{"test3", 1}, &ok)
	xclient.Call(context.Background(), "AddMany", &BitmapValuesRequest{"test3", []uint32{2, 3, 20, 21}}, &ok)

	var exist bool
	xclient.Call(context.Background(), "Exists", &BitmapValueRequest{"test2", 10}, &exist)
	if !exist {
		log.Fatalf("10 not found")
	}

	xclient.Call(context.Background(), "DiffStore", &BitmapDstAndPairRequest{"test4", "test2", "test3"}, &ok)
	xclient.Call(context.Background(), "Exists", &BitmapValueRequest{"test4", 10}, &exist)
	if !exist {
		log.Fatalf("10 not found")
	}
}
