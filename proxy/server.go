package proxy

import "github.com/deatheyes/mongo-proxy/topology"

type Server interface {
	Serve(topology.Connection) error
}
