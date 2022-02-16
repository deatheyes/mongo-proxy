package meta

import (
	"github.com/deatheyes/mongo-proxy/topology"
)

// ClusterInfo contains the necessary utils to build a raw connection to a mongodb
type ClusterInfo struct {
	source    string
	uname     string
	password  string
	mechanism string
	endpoint  topology.EndpointManager
}

func (c *ClusterInfo) Source() string {
	return c.source
}

func (c *ClusterInfo) Uname() string {
	return c.uname
}

func (c *ClusterInfo) Password() string {
	return c.password
}

func (c *ClusterInfo) Mechanism() string {
	return c.mechanism
}

func (c *ClusterInfo) Endpoint() topology.EndpointManager {
	return c.endpoint
}
