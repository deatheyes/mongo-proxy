package proxy

import (
	"context"
	"net"
	"time"

	"github.com/deatheyes/mongo-proxy/auth"
	"github.com/deatheyes/mongo-proxy/meta"
	"github.com/deatheyes/mongo-proxy/topology"
	"github.com/golang/glog"
)

var (
	debug = true
)

func EnableDebug() {
	debug = true
}

func DisableDebug() {
	debug = false
}

func Debug() bool {
	return debug
}

// Porxy is a server that starts wokers for a connection pair.
type Proxy struct {
	clusterMetaClient meta.Client
	authMetaClient    meta.Client
	aliveTimeout      time.Duration
}

func New() *Proxy {
	return &Proxy{}
}

func (p *Proxy) ClusterMetaServer(server meta.Client) *Proxy {
	p.clusterMetaClient = server
	return p
}

func (p *Proxy) AuthMetaServer(server meta.Client) *Proxy {
	p.authMetaClient = server
	return p
}

func (p *Proxy) AliveTimeout(timeout time.Duration) *Proxy {
	p.aliveTimeout = timeout
	return p
}

func (p *Proxy) Serve(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			glog.Errorf("[Proxy][Serve] accept failed: %v", err)
			continue
		}
		p.handle(topology.NewWireConnection(conn))
	}
}

func (p *Proxy) handle(conn topology.Connection) {
	// setup worker
	worker := NewWoker(conn)
	cred := &auth.Cred{
		Salt:      SCRAMSHA256SALT,
		Iteration: SCRAMSHA256ITERATION,
		Lookup: func(uname string) (string, error) {
			password, err := p.lookupPassword(uname)
			if err != nil {
				return "", err
			}
			worker.UID = uname
			return password, nil
		},
	}
	// only support SCRAM-SHA-256 currently.
	authenticator, err := auth.NewScramSHA256ServerAuthenticator(cred)
	if err != nil {
		glog.Errorf("[Proxy][Handle] create authenticator failed: %v", err)
		conn.Close()
		return
	}
	worker.authHandler = auth.NewSaslServerConversation(authenticator.CreateSaslServer())
	worker.metaClient = p.clusterMetaClient
	worker.aliveTimeout = p.aliveTimeout
	worker.Run()
	// TODO: statistic
}

// lookupPassword embeds self-defined authentication into mongo connection handshake.
func (p *Proxy) lookupPassword(uname string) (string, error) {
	token, err := p.authMetaClient.Token(context.Background(), uname)
	if err != nil {
		glog.Errorf("[LookupPassword][%s] get token failed: %v", uname, err)
		return "", err
	}
	return token, nil
}
