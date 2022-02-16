package topology

import (
	"errors"
	"math/rand"
)

// EndpointManager manages endpoints
type EndpointManager interface {
	One(cluster string) (string, error)
	All(cluster string) ([]string, error)
}

type SingleEndpointManager struct {
	endpoint string
}

func NewSingleEndpointManager(endpoint string) *SingleEndpointManager {
	return &SingleEndpointManager{endpoint: endpoint}
}

func (m *SingleEndpointManager) One(_ string) (string, error) {
	return m.endpoint, nil
}

func (m *SingleEndpointManager) All(_ string) ([]string, error) {
	return []string{m.endpoint}, nil
}

type EmptyManager struct{}

func NewEmptyManager() *EmptyManager {
	return &EmptyManager{}
}

func (m *EmptyManager) One(_ string) (string, error) {
	return "", errors.New("empty manager")
}

func (m *EmptyManager) All(_ string) ([]string, error) {
	return nil, errors.New("empty manager")
}

type RandomEndpointManager struct {
	endpoints []string
}

func NewRandomEndpointManager(servers []string) *RandomEndpointManager {
	return &RandomEndpointManager{
		endpoints: servers,
	}
}

func (m *RandomEndpointManager) One(_ string) (string, error) {
	if len(m.endpoints) == 0 {
		return "", errors.New("empty server list")
	}
	id := rand.Intn(len(m.endpoints))
	return m.endpoints[id], nil
}

func (m *RandomEndpointManager) All(_ string) ([]string, error) {
	return m.endpoints, nil
}
