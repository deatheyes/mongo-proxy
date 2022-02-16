package meta

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/deatheyes/mongo-proxy/topology"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	client *mongo.Client
	url    string
	source string
}

func NewMongoClient() *MongoClient {
	return &MongoClient{}
}

func (c *MongoClient) URL(url string) *MongoClient {
	c.url = url
	return c
}

func (c *MongoClient) Source(source string) *MongoClient {
	c.source = source
	return c
}

func (c *MongoClient) Connect(ctx context.Context) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(c.url))
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *MongoClient) ReConnect(ctx context.Context) error {
	newCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	c.client.Disconnect(newCtx)
	return c.client.Connect(newCtx)
}

func (c *MongoClient) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

type ClusterConfig struct {
	ID  primitive.ObjectID `bson:"_id" json:"_id"`
	URI string             `bson:"uri" json:"uri"`
	DB  string             `bson:"db" json:"db"`
}

// Cluster returns the necessary information for router.
func (c *MongoClient) Cluster(ctx context.Context, uid string) (*ClusterInfo, error) {
	clusterConfig := &ClusterConfig{}
	if err := c.client.Database(c.source).
		Collection("configs").
		FindOne(ctx, bson.M{"uid": uid}).
		Decode(clusterConfig); err != nil {
		defer c.ReConnect(context.Background())
		return nil, err
	}

	u, err := url.Parse(clusterConfig.URI)
	if err != nil {
		return nil, err
	}

	info := &ClusterInfo{
		source:    clusterConfig.DB,
		uname:     u.User.Username(),
		mechanism: "SCRAM-SHA-256",
		endpoint:  topology.NewRandomEndpointManager(strings.Split(u.Host, ",")),
	}
	passowrd, ok := u.User.Password()
	if ok {
		info.password = passowrd
	}
	return info, nil
}

type AuthInfo struct {
	ID     primitive.ObjectID `bson:"_id" json:"_id"`
	UID    string             `bson:"uid" json:"uid"`
	Secret string             `bson:"secret" json:"secrte"`
}

// Token returns the auth secret for authenticator.
func (c *MongoClient) Token(ctx context.Context, uid string) (string, error) {
	authInfo := &AuthInfo{}
	if err := c.client.Database(c.source).
		Collection("auths").
		FindOne(ctx, bson.M{"uid": uid}).
		Decode(authInfo); err != nil {
		defer c.ReConnect(context.Background())
		return "", err
	}
	return authInfo.Secret, nil
}
