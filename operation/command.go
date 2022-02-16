package operation

import (
	"context"
	"fmt"

	"github.com/deatheyes/mongo-proxy/topology"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// Command is used to run a generic operation.
type Command struct {
	command           bsoncore.Document
	database          string
	clock             *ClusterTime
	result            bsoncore.Document
	conn              topology.Connection
	processResponseFn func(bsoncore.Document) error
}

// NewCommand constructs and returns a new Command.
func NewCommand(command bsoncore.Document) *Command {
	return &Command{command: command}
}

// Result returns the result of executing this operation.
func (c *Command) Result() bsoncore.Document {
	return c.result
}

// Command sets the command to be run.
func (c *Command) Command(command bsoncore.Document) *Command {
	if c == nil {
		c = new(Command)
	}
	c.command = command
	return c
}

// ClusterClock sets the cluster clock for this operation.
func (c *Command) ClusterClock(clock *ClusterTime) *Command {
	if c == nil {
		c = new(Command)
	}
	c.clock = clock
	return c
}

// Database sets the database to run this operation against.
func (c *Command) Database(database string) *Command {
	if c == nil {
		c = new(Command)
	}
	c.database = database
	return c
}

func (c *Command) ProcessResponse(f func(body bsoncore.Document) error) *Command {
	if c == nil {
		c = new(Command)
	}
	c.processResponseFn = f
	return c
}

// Connection sets the Connection to use for this operation.
func (c *Command) Connection(conn topology.Connection) *Command {
	if c == nil {
		c = new(Command)
	}
	c.conn = conn
	return c
}

func (c *Command) Payload() ([]byte, error) {
	payload := []byte{}
	payload = wiremessage.AppendMsgFlags(payload, 0)
	payload = wiremessage.AppendMsgSectionType(payload, wiremessage.SingleDocument)
	var idx int32
	idx, payload = bsoncore.AppendDocumentStart(payload)
	payload = append(payload, c.command[4:len(c.command)-1]...)
	if c.clock != nil {
		data, err := bson.Marshal(c.clock)
		if err != nil {
			return nil, err
		}
		payload = bsoncore.AppendDocumentElement(payload, "$clusterTime", data)
	}
	if len(c.database) != 0 {
		payload = bsoncore.AppendStringElement(payload, "$db", c.database)
	}
	return bsoncore.AppendDocumentEnd(payload, idx)
}

// Execute runs this operations and returns an error if the operaiton did not execute successfully.
func (c *Command) Execute(ctx context.Context) error {
	payload, err := c.Payload()
	if err != nil {
		return err
	}

	if err := c.conn.WriteWireMessage(ctx, payload, wiremessage.OpMsg); err != nil {
		return err
	}

	res, code, err := c.conn.ReadWireMessage(ctx)
	if err != nil {
		return err
	}
	c.result = res
	if code != wiremessage.OpMsg {
		return fmt.Errorf("unexpect OpCode: %v, expect: %v", code, wiremessage.OpMsg)
	}
	// decode response
	if c.processResponseFn != nil {
		if err := c.processResponseFn(res); err != nil {
			return err
		}
	}
	return nil
}
