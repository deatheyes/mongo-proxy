package meta

import (
	"context"
	"errors"

	"github.com/deatheyes/mongo-proxy/config"
)

// Client gets route and auth information from meta server.
type Client interface {
	Connect(ctx context.Context) error
	// Cluster returns the necessary information for router.
	Cluster(ctx context.Context, uid string) (*ClusterInfo, error)
	// Token returns the auth secret for authenticator.
	Token(ctx context.Context, uid string) (string, error)
	Close(ctx context.Context) error
}

// DefaultInit create a singleton client
func DefaultInit(ctx context.Context, backend *config.Backend) error {
	client, err := CreateClient(backend)
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		return err
	}
	defaultMetaClient = client
	return nil
}

var (
	defaultMetaClient Client
)

// CreateClient returns a meta client.
// Only mongo supported currently.
func CreateClient(backend *config.Backend) (Client, error) {
	if backend.Mongo == nil {
		return nil, errors.New("nil mongo configuration")
	}
	return createMongoClient(backend)
}

func createMongoClient(backend *config.Backend) (Client, error) {
	return NewMongoClient().Source(backend.Mongo.Database).URL(backend.Mongo.URL), nil
}
