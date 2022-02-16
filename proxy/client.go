package proxy

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/deatheyes/mongo-proxy/auth"
	"github.com/deatheyes/mongo-proxy/operation"
	"github.com/deatheyes/mongo-proxy/topology"
)

type ClientOptions struct {
	cluster        string
	appName        string
	source         string
	username       string
	password       string
	connectTimeout time.Duration
}

// SingleEndpointClient build only one connection to a server.
// It represents a connection from proxy to mongo cluster.
type SingleEndpointClient struct {
	ClientOptions
	conn          topology.Connection
	handshakeInfo *operation.HandshakeInformation
}

func NewSingleEndpointClient() *SingleEndpointClient {
	return &SingleEndpointClient{}
}

func (c *SingleEndpointClient) Cluster(cluster string) *SingleEndpointClient {
	if c == nil {
		c = NewSingleEndpointClient()
	}
	c.cluster = cluster
	return c
}

func (c *SingleEndpointClient) AppName(appName string) *SingleEndpointClient {
	if c == nil {
		c = &SingleEndpointClient{}
	}
	c.appName = appName
	return c
}

func (c *SingleEndpointClient) Source(source string) *SingleEndpointClient {
	if c == nil {
		c = &SingleEndpointClient{}
	}
	c.source = source
	return c
}

func (c *SingleEndpointClient) Username(username string) *SingleEndpointClient {
	if c == nil {
		c = &SingleEndpointClient{}
	}
	c.username = username
	return c
}

func (c *SingleEndpointClient) Password(password string) *SingleEndpointClient {
	if c == nil {
		c = &SingleEndpointClient{}
	}
	c.password = password
	return c
}

func (c *SingleEndpointClient) ConnectTimeout(timeout time.Duration) *SingleEndpointClient {
	if c == nil {
		c = &SingleEndpointClient{}
	}
	c.connectTimeout = timeout
	return c
}

func (c *SingleEndpointClient) Connect(m topology.EndpointManager) error {
	endpoint, err := m.One(c.cluster)
	if err != nil {
		return err
	}
	if c.connectTimeout == 0 {
		c.connectTimeout = time.Second
	}
	conn, err := net.DialTimeout("tcp", endpoint, c.connectTimeout)
	if err != nil {
		return err
	}
	c.conn = topology.NewWireConnection(conn)

	// isMaster
	handshakeInfo, err := operation.NewIsMaster().AppName(c.appName).GetHandshakeInformation(context.Background(), c.conn)
	if err != nil {
		return err
	}
	if !handshakeInfo.IsMaster {
		return errors.New("client handshake failed - ismaster:false")
	}
	c.handshakeInfo = handshakeInfo
	return nil
}

// Auth process a client side authenticate handshake.
// Only SCRAM-SHA-256 support currently.
func (c *SingleEndpointClient) Auth(ctx context.Context) error {
	authenticator, err := auth.NewScramSHA256ClientAuthenticator(&auth.Cred{
		Source:   c.source,
		Username: c.username,
		Password: c.password,
	})
	if err != nil {
		return err
	}
	return authenticator.Auth(ctx, &auth.Config{
		Connection:   c.conn,
		ClusterClock: &c.handshakeInfo.ClusterTime,
	})
}
