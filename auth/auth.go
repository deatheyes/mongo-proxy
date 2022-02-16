package auth

import (
	"context"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type Authenticator interface {
	// Auth authenticates the connection.
	Auth(context.Context, *Config) error
}

// SpeculativeAuthenticator represents an authenticator that supports speculative authentication.
type SpeculativeAuthenticator interface {
	CreateSpeculativeConversation() (SpeculativeConversation, error)
}

// SpeculativeConversation represents an authentication conversation that can be merged with the initial connection
// handshake.
//
// FirstMessage method returns the first message to be sent to the server. This message will be included in the initial
// isMaster command.
//
// Finish takes the server response to the initial message and conducts the remainder of the conversation to
// authenticate the provided connection.
type SpeculativeConversation interface {
	FirstMessage() (bsoncore.Document, error)
	Finish(ctx context.Context, cfg *Config, firstResponse bsoncore.Document) error
}

type AuthenticatorHandler interface {
	Next(bsoncore.Document) (bsoncore.Document, error)
	Done() bool
	Source() string
}
