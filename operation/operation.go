package operation

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"

	"github.com/deatheyes/mongo-proxy/topology"
	"github.com/deatheyes/mongo-proxy/version"
	"github.com/deatheyes/mongo-proxy/wire"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// ClusterTime is a timestamp signature
// Note:
// Handshakes without ClusterTime would be rejected by some driver such as node.
// If come up with these cases, preconnection is necessary to be built up to get the server side ClusterTime while handshaking.
type ClusterTime struct {
	Timestamp primitive.Timestamp `bson:"clusterTime"`
	Signature bson.M              `bson:"signature"`
}

// HandshakeInformation is response for handshake.
type HandshakeInformation struct {
	IsMaster              bool                `bson:"ismaster"`
	Message               string              `bson:"msg"`
	MaxDocumentSize       uint32              `bson:"maxBsonObjectSize"`
	MaxMessageSize        uint32              `bson:"maxMessageSizeBytes"`
	MaxBatchCount         uint32              `bson:"maxWriteBatchSize"`
	LocalTime             primitive.DateTime  `bson:"localTime"`
	SessionTimeoutMinutes uint32              `bson:"logicalSessionTimeoutMinutes"`
	MaxWireVersion        int32               `bson:"maxWireVersion"`
	MinWireVersion        int32               `bson:"minWireVersion"`
	OK                    float32             `bson:"ok"`
	OperationTime         primitive.Timestamp `bson:"operationTime"`
	ClusterTime           ClusterTime         `bson:"$clusterTime"`
}

func (i *HandshakeInformation) String() string {
	content, _ := bson.Marshal(i)
	return bsoncore.Document(content).String()
}

// IsMaster is the first request for handshake and authentication.
type IsMaster struct {
	appname            string
	compressors        []string
	saslSupportedMechs string
	clock              *ClusterTime
	speculativeAuth    bsoncore.Document
	maxAwaitTimeMS     *int64

	res bsoncore.Document
}

// NewIsMaster constructs an IsMaster.
func NewIsMaster() *IsMaster {
	return &IsMaster{}
}

// AppName sets the application name in the client metadata sent in this operation.
func (im *IsMaster) AppName(appname string) *IsMaster {
	im.appname = appname
	return im
}

// ClusterClock sets the cluster clock for this operation.
func (im *IsMaster) ClusterClock(clock *ClusterTime) *IsMaster {
	if im == nil {
		im = new(IsMaster)
	}

	im.clock = clock
	return im
}

// Compressors sets the compressors that can be used.
func (im *IsMaster) Compressors(compressors []string) *IsMaster {
	im.compressors = compressors
	return im
}

// SASLSupportedMechs retrieves the supported SASL mechanism for the given user when this operation
// is run.
func (im *IsMaster) SASLSupportedMechs(username string) *IsMaster {
	im.saslSupportedMechs = username
	return im
}

// SpeculativeAuthenticate sets the document to be used for speculative authentication.
func (im *IsMaster) SpeculativeAuthenticate(doc bsoncore.Document) *IsMaster {
	im.speculativeAuth = doc
	return im
}

func (im *IsMaster) decodeStringSlice(element bsoncore.Element, name string) ([]string, error) {
	arr, ok := element.Value().ArrayOK()
	if !ok {
		return nil, fmt.Errorf("expected '%s' to be an array but it's a BSON %s", name, element.Value().Type)
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, err
	}
	var strs []string
	for _, val := range vals {
		str, ok := val.StringValueOK()
		if !ok {
			return nil, fmt.Errorf("expected '%s' to be an array of strings, but found a BSON %s", name, val.Type)
		}
		strs = append(strs, str)
	}
	return strs, nil
}

func (im *IsMaster) decodeStringMap(element bsoncore.Element, name string) (map[string]string, error) {
	doc, ok := element.Value().DocumentOK()
	if !ok {
		return nil, fmt.Errorf("expected '%s' to be a document but it's a BSON %s", name, element.Value().Type)
	}
	elements, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for _, element := range elements {
		key := element.Key()
		value, ok := element.Value().StringValueOK()
		if !ok {
			return nil, fmt.Errorf("expected '%s' to be a document of strings, but found a BSON %s", name, element.Value().Type)
		}
		m[key] = value
	}
	return m, nil
}

// command appends all necessary command fields.
func (im *IsMaster) command(dst []byte) ([]byte, error) {
	dst = bsoncore.AppendInt32Element(dst, "isMaster", 1)
	if im.saslSupportedMechs != "" {
		dst = bsoncore.AppendStringElement(dst, "saslSupportedMechs", im.saslSupportedMechs)
	}
	if im.speculativeAuth != nil {
		dst = bsoncore.AppendDocumentElement(dst, "speculativeAuthenticate", im.speculativeAuth)
	}
	var idx int32
	idx, dst = bsoncore.AppendArrayElementStart(dst, "compression")
	for i, compressor := range im.compressors {
		dst = bsoncore.AppendStringElement(dst, strconv.Itoa(i), compressor)
	}
	dst, _ = bsoncore.AppendArrayEnd(dst, idx)

	// append client metadata
	idx, dst = bsoncore.AppendDocumentElementStart(dst, "client")

	didx, dst := bsoncore.AppendDocumentElementStart(dst, "driver")
	dst = bsoncore.AppendStringElement(dst, "name", "mongo-go-proxy")
	dst = bsoncore.AppendStringElement(dst, "version", version.Driver)
	dst, _ = bsoncore.AppendDocumentEnd(dst, didx)

	didx, dst = bsoncore.AppendDocumentElementStart(dst, "os")
	dst = bsoncore.AppendStringElement(dst, "type", runtime.GOOS)
	dst = bsoncore.AppendStringElement(dst, "architecture", runtime.GOARCH)
	dst, _ = bsoncore.AppendDocumentEnd(dst, didx)

	dst = bsoncore.AppendStringElement(dst, "platform", runtime.Version())
	if im.appname != "" {
		didx, dst = bsoncore.AppendDocumentElementStart(dst, "application")
		dst = bsoncore.AppendStringElement(dst, "name", im.appname)
		dst, _ = bsoncore.AppendDocumentEnd(dst, didx)
	}
	dst, _ = bsoncore.AppendDocumentEnd(dst, idx)
	return dst, nil
}

// RequestPalyload create the request body
func (im *IsMaster) RequestPayload() ([]byte, error) {
	payload := []byte{}
	payload = wiremessage.AppendQueryFlags(payload, wiremessage.SlaveOK)
	payload = wiremessage.AppendQueryFullCollectionName(payload, "admin.$cmd")
	payload = wiremessage.AppendQueryNumberToSkip(payload, 0)
	payload = wiremessage.AppendQueryNumberToReturn(payload, -1)
	var idx int32
	var err error
	idx, payload = bsoncore.AppendDocumentStart(payload)
	payload, err = im.command(payload)
	if err != nil {
		return nil, err
	}
	payload, _ = bsoncore.AppendDocumentEnd(payload, idx)
	return payload, nil
}

func (im *IsMaster) GetHandshakeInformation(ctx context.Context, c topology.Connection) (*HandshakeInformation, error) {
	payload, err := im.RequestPayload()
	if err != nil {
		return nil, err
	}

	if err := c.WriteWireMessage(ctx, payload, wiremessage.OpQuery); err != nil {
		return nil, err
	}

	res, code, err := c.ReadWireMessage(ctx)
	if err != nil {
		return nil, err
	}
	if code != wiremessage.OpReply {
		return nil, fmt.Errorf("unexpected op code: %v, expected: %v", code, wiremessage.OpReply)
	}

	replyInfo, err := wire.DecodeReply(res)
	if err != nil {
		return nil, err
	}

	if len(replyInfo.Docs) == 0 {
		return nil, errors.New("no documents in reply info")
	}

	handshakeInfo := &HandshakeInformation{}
	if err := bson.Unmarshal(replyInfo.Docs[0], handshakeInfo); err != nil {
		return nil, err
	}
	return handshakeInfo, nil
}
