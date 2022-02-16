package operation_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/deatheyes/mongo-proxy/operation"
	"github.com/deatheyes/mongo-proxy/topology"
	"github.com/deatheyes/mongo-proxy/wire"
)

func TestIsMaster(t *testing.T) {
	conn, err := net.Dial("tcp", ":27019")
	if err != nil {
		t.Error("connect failed:", err)
		return
	}
	defer conn.Close()

	banckend := topology.NewWireConnection(conn)
	op := operation.NewIsMaster()
	info, err := op.GetHandshakeInformation(context.Background(), banckend)
	if err != nil {
		t.Error("get handshake info failed:", err)
		return
	}
	fmt.Println("INFO:", info.String())
}

func TestIsMasterWireMessage(t *testing.T) {
	op := operation.NewIsMaster()
	data, err := op.RequestPayload()
	if err != nil {
		t.Error("create request palyload failed:", err)
		return
	}

	if _, err := wire.DecodeQuery(data); err != nil {
		t.Error("process query failed:", err)
		return
	}
}
