package auth

import (
	"context"
	"fmt"

	"github.com/xdg-go/scram"
	"github.com/xdg-go/stringprep"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

const (
	// SCRAMSHA256 holds the mechanism name "SCRAM-SHA-256"
	SCRAMSHA256 = "SCRAM-SHA-256"
)

var (
	// Additional options for the saslStart command to enable a shorter SCRAM conversation
	scramStartOptions bsoncore.Document = bsoncore.BuildDocumentFromElements(nil,
		bsoncore.AppendBooleanElement(nil, "skipEmptyExchange", true),
	)
)

func NewScramSHA256ClientAuthenticator(cred *Cred) (Authenticator, error) {
	passprep, err := stringprep.SASLprep.Prepare(cred.Password)
	if err != nil {
		return nil, fmt.Errorf("error SASLprepping password '%s' - %v", cred.Password, err)
	}
	client, err := scram.SHA256.NewClientUnprepped(cred.Username, passprep, "")
	if err != nil {
		return nil, fmt.Errorf("error initializing SCRAM-SHA-256 client - %v", err)
	}
	client.WithMinIterations(4096)
	return &ScramClientAuthenticator{
		mechanism: SCRAMSHA256,
		source:    cred.Source,
		client:    client,
	}, nil
}

// ScramClientAuthenticator uses the SCRAM algorithm over SASL to authenticate a connection.
type ScramClientAuthenticator struct {
	mechanism string
	source    string
	client    *scram.Client
}

// Auth authenticates the provided connection by conducting a full SASL conversation.
func (a *ScramClientAuthenticator) Auth(ctx context.Context, cfg *Config) error {
	err := ConductSaslClientConversation(ctx, cfg, a.source, a.CreateSaslClient())
	if err != nil {
		return fmt.Errorf("sasl client conversation failed: %v", err)
	}
	return nil
}

// CreateSpeculativeConversation creates a speculative conversation for SCRAM authentication.
func (a *ScramClientAuthenticator) CreateSpeculativeConversation() (SpeculativeConversation, error) {
	return newSaslClientConversation(a.CreateSaslClient(), a.source, true), nil
}

func (a *ScramClientAuthenticator) CreateSaslClient() SaslClient {
	return &scramSaslAdapter{
		conversation: a.client.NewConversation(),
		mechanism:    a.mechanism,
	}
}

type scramSaslAdapter struct {
	mechanism    string
	conversation *scram.ClientConversation
}

func (a *scramSaslAdapter) Start() (string, []byte, error) {
	step, err := a.conversation.Step("")
	if err != nil {
		return a.mechanism, nil, err
	}
	return a.mechanism, []byte(step), nil
}

func (a *scramSaslAdapter) Next(challenge []byte) ([]byte, error) {
	step, err := a.conversation.Step(string(challenge))
	if err != nil {
		return nil, err
	}
	return []byte(step), nil
}

func (a *scramSaslAdapter) Completed() bool {
	return a.conversation.Done()
}

func (*scramSaslAdapter) StartCommandOptions() bsoncore.Document {
	return scramStartOptions
}

func NewScramSHA256ServerAuthenticator(cred *Cred) (*ScramServerAuthenticator, error) {
	ret := &ScramServerAuthenticator{
		mechanism: SCRAMSHA256,
		iteration: cred.Iteration,
		salt:      cred.Salt,
	}
	server, err := scram.SHA256.NewServer(ret.LookupPassword(cred.Lookup))
	if err != nil {
		return nil, err
	}
	ret.server = server
	return ret, nil
}

// ScramAuthenticator uses the SCRAM algorithm over SASL to authenticate a connection.
type ScramServerAuthenticator struct {
	mechanism string
	source    string
	server    *scram.Server
	salt      string
	iteration int
}

func (a *ScramServerAuthenticator) CreateSaslServer() SaslServer {
	return &scramSaslServerAdapter{
		conversation: a.server.NewConversation(),
		mechanism:    a.mechanism,
	}
}

// PasswordLookup get password by username
type PasswordLookup func(uname string) (password string, err error)

// LookupPassword create a scram.CredentialLookup with uname and password
func (a *ScramServerAuthenticator) LookupPassword(lookup func(uname string) (password string, err error)) scram.CredentialLookup {
	return func(s string) (scram.StoredCredentials, error) {
		password, err := lookup(s)
		if err != nil {
			return scram.StoredCredentials{}, err
		}
		client, err := scram.SHA256.NewClient(s, password, "")
		client.WithMinIterations(4096)
		kf := scram.KeyFactors{Salt: a.salt, Iters: a.iteration}
		return client.GetStoredCredentials(kf), nil
	}
}

type scramSaslServerAdapter struct {
	mechanism    string
	conversation *scram.ServerConversation
}

func (a *scramSaslServerAdapter) Next(challenge []byte) ([]byte, error) {
	step, err := a.conversation.Step(string(challenge))
	if err != nil {
		return nil, err
	}
	return []byte(step), nil
}

func (a *scramSaslServerAdapter) Completed() bool {
	return a.conversation.Done()
}
