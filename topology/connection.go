package topology

import (
	"context"
	"net"
	"time"

	"github.com/deatheyes/mongo-proxy/wire"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

var defaultMaxMessageSize uint32 = 48000000

// Connection represents the upstream or downstream connection for mongodb
type Connection interface {
	// WriteWireMessage write the message as the type specified by OpCode
	WriteWireMessage(context.Context, []byte, wiremessage.OpCode) error
	// ReadWireMessage read the message and its type
	ReadWireMessage(ctx context.Context) ([]byte, wiremessage.OpCode, error)
	// RequestID return the current requestID binds to the connection
	// Note:
	// 1. client connection(client side handshake connection for mongo) should always increase the requestID every time create a request
	// 2. proxy connection(connection not in handshake) and server side handshake connection for driver must mirror the requestID.
	RequestID() int32
	SetRequestID(int32)
	// ResponseID return the current responseID binds to the connection
	// Note:
	// 1. server connection(server side handshake connection for driver) should always create a new reponseID every time create a response
	// 2. proxy connection(connection not in handshake) must mirror the responseID
	ResponseID() int32
	SetResponseID(int32)
	Close() error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

type WireConnection struct {
	conn       net.Conn
	requestID  int32
	responseID int32
}

func NewWireConnection(conn net.Conn) *WireConnection {
	return &WireConnection{
		conn: conn,
	}
}

func (c *WireConnection) WriteWireMessage(ctx context.Context, body []byte, code wiremessage.OpCode) error {
	header := &wire.Header{
		MessageLength: int32(len(body)) + 16,
		RequestID:     c.requestID,
		ResponseTo:    c.responseID,
		OpCode:        code,
	}
	// TODO with context
	_, err := header.WriteTo(c.conn)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(body)
	if err != nil {
		return err
	}
	return nil
}

func (c *WireConnection) ReadWireMessage(ctx context.Context) ([]byte, wiremessage.OpCode, error) {
	header, err := wire.ReadHead(c.conn)
	if err != nil {
		return nil, 0, err
	}
	c.requestID = header.RequestID
	c.responseID = header.ResponseTo
	body, err := wire.ReadBody(header, c.conn)
	return body, header.OpCode, err
}

func (c *WireConnection) RequestID() int32 {
	return c.requestID
}

func (c *WireConnection) ResponseID() int32 {
	return c.responseID
}

func (c *WireConnection) SetRequestID(id int32) {
	c.requestID = id
}

func (c *WireConnection) SetResponseID(id int32) {
	c.responseID = id
}

func (c *WireConnection) Close() error {
	return c.conn.Close()
}

func (c *WireConnection) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}

func (c *WireConnection) SetWriteDeadline(deadline time.Time) error {
	return c.conn.SetWriteDeadline(deadline)
}
