package auth

import (
	"context"
	"fmt"

	"github.com/deatheyes/mongo-proxy/operation"
	"github.com/deatheyes/mongo-proxy/wire"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// SaslClient is the client piece of a sasl conversation.
type SaslClient interface {
	Start() (string, []byte, error)
	Next(challenge []byte) ([]byte, error)
	Completed() bool
}

// SaslServer is the server piece of a sasl conversation.
type SaslServer interface {
	Next(challenge []byte) ([]byte, error)
	Completed() bool
}

// ExtraOptionsSaslClient is a SaslClient that appends options to the saslStart command.
type ExtraOptionsSaslClient interface {
	StartCommandOptions() bsoncore.Document
}

// saslClientConversation represents a SASL client conversation. This type implements the SpeculativeConversation interface so the
// conversation can be executed in multi-step speculative fashion.
type saslClientConversation struct {
	client      SaslClient
	source      string
	mechanism   string
	speculative bool
	response    *saslResponse
}

func newSaslClientConversation(client SaslClient, source string, speculative bool) *saslClientConversation {
	authSource := source
	if authSource == "" {
		authSource = defaultAuthDB
	}
	return &saslClientConversation{
		client:      client,
		source:      authSource,
		speculative: speculative,
	}
}

// FirstMessage returns the first message to be sent to the server. This message contains a "db" field so it can be used
// for speculative authentication.
func (sc *saslClientConversation) FirstMessage() (bsoncore.Document, error) {
	var payload []byte
	var err error
	sc.mechanism, payload, err = sc.client.Start()
	if err != nil {
		return nil, err
	}

	saslCmdElements := [][]byte{
		bsoncore.AppendInt32Element(nil, "saslStart", 1),
		bsoncore.AppendStringElement(nil, "mechanism", sc.mechanism),
		bsoncore.AppendBinaryElement(nil, "payload", 0x00, payload),
	}
	if sc.speculative {
		// The "db" field is only appended for speculative auth because the isMaster command is executed against admin
		// so this is needed to tell the server the user's auth source. For a non-speculative attempt, the SASL commands
		// will be executed against the auth source.
		saslCmdElements = append(saslCmdElements, bsoncore.AppendStringElement(nil, "db", sc.source))
	}
	if extraOptionsClient, ok := sc.client.(ExtraOptionsSaslClient); ok {
		optionsDoc := extraOptionsClient.StartCommandOptions()
		saslCmdElements = append(saslCmdElements, bsoncore.AppendDocumentElement(nil, "options", optionsDoc))
	}

	return bsoncore.BuildDocumentFromElements(nil, saslCmdElements...), nil
}

type saslResponse struct {
	ConversationID int    `bson:"conversationId"`
	Code           int    `bson:"code"`
	Done           bool   `bson:"done"`
	Payload        []byte `bson:"payload"`
}

func (sc *saslClientConversation) decodeResponse(body bsoncore.Document) error {
	msgInfo, err := wire.DecodeMSG(body)
	if err != nil {
		return fmt.Errorf("decode MSG failed - %v", err)
	}

	sc.response = &saslResponse{}
	if err := bson.Unmarshal(msgInfo.Document(), sc.response); err != nil {
		return fmt.Errorf("unmarshal failed: %v mechanism: %s", err, sc.mechanism)
	}
	return nil
}

// Finish completes the conversation based on the first server response to authenticate the given connection.
func (sc *saslClientConversation) Finish(ctx context.Context, cfg *Config, firstResponse bsoncore.Document) error {
	var err error
	var payload []byte
	for {
		saslResp := sc.response
		cid := saslResp.ConversationID
		if saslResp.Code != 0 {
			return fmt.Errorf("code: %d mechanism: %s", saslResp.Code, sc.mechanism)
		}

		if saslResp.Done && sc.client.Completed() {
			return nil
		}

		payload, err = sc.client.Next(saslResp.Payload)
		if err != nil {
			return fmt.Errorf("next failed: %v mechanism: %s", err, sc.mechanism)
		}

		if saslResp.Done && sc.client.Completed() {
			return nil
		}

		doc := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendInt32Element(nil, "saslContinue", 1),
			bsoncore.AppendInt32Element(nil, "conversationId", int32(cid)),
			bsoncore.AppendBinaryElement(nil, "payload", 0x00, payload),
		)
		saslContinueCmd := operation.NewCommand(doc).
			Database(sc.source).
			Connection(cfg.Connection).
			ClusterClock(cfg.ClusterClock).
			ProcessResponse(sc.decodeResponse)

		err = saslContinueCmd.Execute(ctx)
		if err != nil {
			return fmt.Errorf("execute failed: %v mechanism: %s", err, sc.mechanism)
		}
	}
}

// ConductSaslConversation runs a full SASL conversation to authenticate the given connection.
func ConductSaslClientConversation(ctx context.Context, cfg *Config, authSource string, client SaslClient) error {
	// Create a non-speculative SASL conversation.
	conversation := newSaslClientConversation(client, authSource, false)

	saslStartDoc, err := conversation.FirstMessage()
	if err != nil {
		return fmt.Errorf("first message failed: %v mechanism: %s", err, conversation.mechanism)
	}
	saslStartCmd := operation.NewCommand(saslStartDoc).
		Database(authSource).
		Connection(cfg.Connection).
		ClusterClock(cfg.ClusterClock).
		ProcessResponse(conversation.decodeResponse)
	if err := saslStartCmd.Execute(ctx); err != nil {
		return fmt.Errorf("start command execute failed: %v mechanism: %s", err, conversation.mechanism)
	}
	return conversation.Finish(ctx, cfg, saslStartCmd.Result())
}

func SaslFirstCommand(authSource string, cfg *Config, client SaslClient) (*operation.Command, error) {
	conversation := newSaslClientConversation(client, authSource, false)
	saslStartDoc, err := conversation.FirstMessage()
	if err != nil {
		return nil, fmt.Errorf("first message failed: %v mechanism: %s", err, conversation.mechanism)
	}
	return operation.NewCommand(saslStartDoc).
		Database(authSource).
		Connection(cfg.Connection).
		ClusterClock(cfg.ClusterClock), nil
}

func ConductSaslServerConversation(ctx context.Context, cfg *Config, server SaslServer) error {
	// Create a non-speculative SASL conversation.
	//conversation := newSaslServerConversation(server)
	return nil
}

type saslRequest struct {
	ConversationID int    `bson:"conversationId"`
	Payload        []byte `bson:"payload"`
	Database       string `bson:"$db"`
	DatabaseAlias  string `bson:"db"`
}

func (s *saslRequest) DatabaseName() string {
	if len(s.Database) != 0 {
		return s.Database
	}
	return s.DatabaseAlias
}

// saslClientConversation represents a SASL server conversation.
type SaslServerConversation struct {
	server    SaslServer
	source    string
	mechanism string
	id        int
}

func NewSaslServerConversation(server SaslServer) *SaslServerConversation {
	return &SaslServerConversation{
		server: server,
	}
}

func (sc *SaslServerConversation) Next(body bsoncore.Document) (bsoncore.Document, error) {
	request := &saslRequest{}
	if err := bson.Unmarshal(body, request); err != nil {
		return nil, err
	}

	if len(request.DatabaseName()) != 0 {
		sc.source = request.DatabaseName()
	}

	if request.ConversationID == 0 {
		// sasl start
		sc.id++
	} else if request.ConversationID != int(sc.id) {
		return bsoncore.Document{}, fmt.Errorf("unexpected conversationID: %v, expect: %v", request.ConversationID, sc.id)
	}

	payload, err := sc.server.Next(request.Payload)
	if err != nil {
		return bsoncore.Document{}, err
	}

	doc := bsoncore.BuildDocumentFromElements(nil,
		bsoncore.AppendInt32Element(nil, "conversationId", int32(sc.id)),
		bsoncore.AppendBooleanElement(nil, "done", sc.server.Completed()),
		bsoncore.AppendBinaryElement(nil, "payload", 0x00, payload),
		bsoncore.AppendDoubleElement(nil, "ok", 1.0),
	)
	return doc, nil
}

func (sc *SaslServerConversation) Done() bool {
	return sc.server.Completed()
}

func (sc *SaslServerConversation) Source() string {
	return sc.source
}
