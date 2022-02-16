package auth

import (
	"github.com/deatheyes/mongo-proxy/operation"
	"github.com/deatheyes/mongo-proxy/topology"
)

// Config holds the information necessary to perform an authentication attempt.
type Config struct {
	Connection   topology.Connection
	ClusterClock *operation.ClusterTime
}

// Cred is a credential.
type Cred struct {
	// client config
	Source   string
	Username string
	Password string
	// server config
	Salt      string
	Iteration int
	Lookup    PasswordLookup
}
