package proxy

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/deatheyes/mongo-proxy/auth"
	"github.com/deatheyes/mongo-proxy/meta"
	"github.com/deatheyes/mongo-proxy/operation"
	"github.com/deatheyes/mongo-proxy/topology"
	"github.com/deatheyes/mongo-proxy/wire"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/atomic"
)

type Message struct {
	Code wiremessage.OpCode
	Body []byte
}

type Context struct {
	ctx      context.Context
	Request  *Message
	Response *Message
	Err      error
	Quit     bool
}

// Worker is responsible for message transmission.
// Necessary authentication, handshake and transformation will be processed.
/*
client  ->   handleFuncChain [HandleCommand   ----> Transform]
                                   |                    |
        <--------------	    block and process      has response? ---> mongo
                                                        | Y              |
        <---------------------------------------     return              |
                                                                         |
        <---------------------------------------     revert     <--- response
*/
type Worker struct {
	metaClient      meta.Client               // meta client to get router info
	upstream        topology.Connection       // user client connection
	downstream      topology.Connection       // cluster connection
	authHandler     auth.AuthenticatorHandler // authenticator
	handleFuncChain []HandlerFunc             // functions sequence to process request from upstream
	wrapFuncDict    map[string]TransformFunc  // transformer for requests from upstream
	revertFuncDict  map[string]TransformFunc  // transformer for responses form downstream
	UID             string                    // client side uname and database
	targetDB        string                    // cluster side database
	stop            chan struct{}             // channel to stop worker
	closed          atomic.Bool               // closed flag
	// Some drivers, such as node's driver has strict verification on clusterTime.
	// Downstream must be built up ASAP on these cases.
	clusterTime  operation.ClusterTime // clusterTime for downstream
	preConnect   bool                  // try to build downstream ASAP
	aliveTimeout time.Duration         // shutdown woker if there is no request in this duration
	lastTime     time.Time             // last accessed time
}

// NewWoker create a woker with upstream
func NewWoker(conn topology.Connection) *Worker {
	w := &Worker{
		upstream: conn,
		stop:     make(chan struct{}),
	}
	w.handleFuncChain = []HandlerFunc{w.HandleCommand, w.TransformRequest}
	// TODO: dropdatabase
	w.wrapFuncDict = map[string]TransformFunc{
		"find":            WrapCommand("find"),
		"findandmodify":   WrapCommand("findAndModify"),
		"insert":          WrapCommand("insert"),
		"killcursors":     WrapCommand("killCursors"),
		"aggregate":       WrapCommand("aggregate"),
		"create":          WrapCommand("create"),
		"createindexes":   WrapCommand("createIndexes"),
		"delete":          WrapCommand("delete"),
		"distinct":        WrapCommand("distinct"),
		"drop":            WrapCommand("drop"),
		"dropindexes":     WrapCommand("dropIndexes"),
		"listindexes":     WrapCommand("listIndexes"),
		"update":          WrapCommand("update"),
		"listcollections": WrapListCollection,
		"getmore":         WrapGetMore,
	}
	w.revertFuncDict = map[string]TransformFunc{
		"cursor": RevertCursor,
	}
	return w
}

func (w *Worker) WithAliveTimeout(timeout time.Duration) *Worker {
	w.aliveTimeout = timeout
	return w
}

// Stop kill this worker
func (w *Worker) Stop() {
	if w.closed.CAS(false, true) {
		close(w.stop)
	}
}

// Run starts the upstream loop and setup the cancel routine
func (w *Worker) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	go w.ServeUpstream(ctx)
	go func() {
		<-w.stop
		cancel()
	}()
}

// RegisterHandleFunc add a upstream request handle function to the sequence
func (w *Worker) RegisterHandleFunc(h HandlerFunc) {
	w.handleFuncChain = append(w.handleFuncChain, h)
}

// RegisterWrapFunc add a transformer triggered by 'key' in upstream request
func (w *Worker) RegisterWrapFunc(key string, f TransformFunc) {
	w.wrapFuncDict[key] = f
}

// RegisterRevertFunc add a transformer triggered by 'key' in downstream response
func (w *Worker) RegisterRevertFunc(key string, f TransformFunc) {
	w.revertFuncDict[key] = f
}

type responseOptions struct {
	Code      wiremessage.OpCode
	Documents []bsoncore.Document
	Flag      int32
	CursorID  int64
	StartFrom int32
	NumReturn int32
}

func (w *Worker) buildResponse(opt *responseOptions) *Message {
	switch opt.Code {
	case wiremessage.OpMsg:
		return &Message{
			Code: wiremessage.OpMsg,
			Body: wire.EncodeMSG(&wire.MSGInfo{
				Flag: wiremessage.MsgFlag(opt.Flag),
				Kind: wiremessage.SingleDocument,
				Docs: opt.Documents,
			}),
		}
	case wiremessage.OpReply:
		return &Message{
			Code: wiremessage.OpReply,
			Body: wire.EncodeReply(&wire.ReplyInfo{
				ReplyFlag: wiremessage.AwaitCapable,
				CursorID:  opt.CursorID,
				StartFrom: opt.StartFrom,
				NumReturn: opt.NumReturn,
				Docs:      opt.Documents,
			}),
		}
	}
	return nil
}

// HandleCommand serves and blocks handshake commands
func (w *Worker) HandleCommand(ctx *Context) {
	// OpMsg <-> OpMsg
	// OpQuery <-> OpReply
	var request bsoncore.Document
	var responseCode wiremessage.OpCode
	switch ctx.Request.Code {
	case wiremessage.OpMsg:
		info, err := wire.DecodeMSG(ctx.Request.Body)
		if err != nil {
			ctx.Response = &Message{
				Code: wiremessage.OpMsg,
				Body: wire.EncodeMSG(ErrorMsg("decode request failed")),
			}
			return
		}
		request = info.Document()
		responseCode = wiremessage.OpMsg
	case wiremessage.OpQuery:
		// debug
		if Debug() {
			glog.V(8).Infof("[HandleCommand] %s", wire.DebugOp(ctx.Request.Body, wiremessage.OpQuery))
		}

		info, err := wire.DecodeQuery(ctx.Request.Body)
		if err != nil {
			ctx.Response = &Message{
				Code: wiremessage.OpCommandReply,
				Body: wire.EncodeReply(ErrorReply("decode request failed")),
			}
			return
		}
		request = info.Query
		responseCode = wiremessage.OpReply
	}

	command, err := CommandName(request)
	if err != nil {
		// not a string command, skip to next handler
		glog.Warningf("[HandleCommand] get command name failed: %v, skip to next handler", err)
		return
	}

	switch command {
	case wire.CommandIsMaster:
		response, err := w.Handshake(request)
		if err != nil {
			glog.Errorf("[HandleCommand][IsMaster] handshake failed: %v", err)
		}
		ctx.Response = w.buildResponse(&responseOptions{
			Code:      responseCode,
			Flag:      int32(wiremessage.AwaitCapable),
			NumReturn: 1,
			Documents: []bsoncore.Document{response},
		})
	case wire.CommandSaslStart, wire.CommandSaslContinue:
		response, err := w.Auth(ctx.ctx, request)
		if err != nil {
			glog.Errorf("[HandleCommand][Auth] handshake failed: %v", err)
		}
		ctx.Response = w.buildResponse(&responseOptions{
			Code:      responseCode,
			NumReturn: 1,
			Documents: []bsoncore.Document{response},
		})
	default:
		// TODO: depraved message type
	}
}

func (w *Worker) Authed() bool {
	return w.authHandler.Done()
}

// Auth processes a server side sals authentication
func (w *Worker) Auth(ctx context.Context, document bsoncore.Document) (bsoncore.Document, error) {
	if w.authHandler.Done() {
		glog.Errorf("[Worker][Auth][%s] duplicated auth", w.UID)
		return ErrorResponse(fmt.Sprintf("duplicated auth on %s", w.UID)), errors.New("duplicated auth")
	}

	response, err := w.authHandler.Next(document)
	if err != nil {
		glog.Errorf("[Worker][Auth][%s] auth failed: %v", w.UID, err)
		return ErrorResponse(fmt.Sprintf("auth failed on %s", w.UID)), err
	}

	if w.authHandler.Done() {
		if !w.preConnect {
			if err := w.ConnectBackend(ctx); err != nil {
				return ErrorResponse("connect backend failed"), err
			}
		}
		glog.Errorf("[Worker][Auth][%s] auth success", w.UID)
	}
	return response, nil
}

func (w *Worker) Handshake(document bsoncore.Document) (bsoncore.Document, error) {
	// speculativeAuthenticate
	TransformElement(document, "speculativeAuthenticate", func(e1 []byte) []byte {
		element := bsoncore.Element(e1)
		if err := element.Validate(); err != nil {
			return e1
		}
		if element.Value().Type != bsontype.EmbeddedDocument {
			return e1
		}
		TransformElement(element.Value().Document(), "db", func(e2 []byte) []byte {
			dbElement := bsoncore.Element(e2)
			if err := dbElement.Validate(); err != nil {
				return e2
			}
			if dbElement.Value().Type != bsontype.String {
				return e2
			}
			w.UID = dbElement.Value().StringValue()
			w.preConnect = true
			return e2
		})
		return e1
	})
	if w.preConnect {
		// build downstream connection to get ClusterTime as soon as we get the UID.
		if err := w.ConnectBackend(context.Background()); err != nil {
			return ErrorResponse("connect to backend failed"), err
		}
	}

	// ismaster
	now := time.Now()
	handshakeInfo := &operation.HandshakeInformation{
		IsMaster:              true,
		Message:               "isdbgrid",
		MaxDocumentSize:       16777216,
		MaxMessageSize:        48000000,
		MaxBatchCount:         100000,
		LocalTime:             primitive.NewDateTimeFromTime(now),
		SessionTimeoutMinutes: 30,
		MaxWireVersion:        7,
		MinWireVersion:        7,
		OK:                    1.0,
		OperationTime:         primitive.Timestamp{T: uint32(now.Unix()), I: uint32(now.Nanosecond() / 1000 % 1000)},
		ClusterTime:           w.clusterTime,
	}
	handshakeDocument, err := bson.Marshal(handshakeInfo)
	if err != nil {
		return ErrorResponse("connect to backend failed"), err
	}

	return handshakeDocument, nil
}

// TransformRequest transforms upstream request to downstream request
func (w *Worker) TransformRequest(ctx *Context) {
	if !w.Authed() {
		// block all requests if unauthorized
		ctx.Response = &Message{
			Code: wiremessage.OpMsg,
			Body: wire.EncodeMSG(ErrorMsg("unauthorized")),
		}
		ctx.Quit = true
	}

	// transform request
	switch ctx.Request.Code {
	case wiremessage.OpMsg:
		info, err := wire.DecodeMSG(ctx.Request.Body)
		if err != nil {
			ctx.Response = &Message{
				Code: wiremessage.OpMsg,
				Body: wire.EncodeMSG(ErrorMsg("decode request failed")),
			}
		}
		document := info.Document()
		command, err := CommandName(document)
		if err != nil {
			glog.Warningf("[TransformRequest] get command name failed: %v", err)
			return
		}
		if f, ok := w.wrapFuncDict[command]; ok {
			info.Docs = []bsoncore.Document{f(w, document)}
			ctx.Request.Body = wire.EncodeMSG(info)
		}
	default:
		// transform other message
	}
}

// TransformResponse transforms downstream response to upstream response
func (w *Worker) TransformResponse(ctx *Context) {
	// transform response
	// only 'OpMsg' is processed currently
	// do nothing but feed back directly if come up with an error
	// TOOD: maybe we should return a error document in case of explosing sensitive infomation
	switch ctx.Response.Code {
	case wiremessage.OpMsg:
		info, err := wire.DecodeMSG(ctx.Response.Body)
		if err != nil {
			glog.Warningf("[Worker][TransformResponse][%s] decode message failed: %v", w.UID, err)
			return
		}
		document := info.Document()
		command, err := CommandName(document)
		if err != nil {
			return
		}
		if f, ok := w.revertFuncDict[command]; ok {
			info.Docs = []bsoncore.Document{f(w, document)}
			ctx.Response.Body = wire.EncodeMSG(info)
		}
	default:
		// transform other message
	}
}

// ServeUpstream block or proxy request to the backend
// If Context.Response is nil, setp into the next handlerFunc
func (w *Worker) ServeUpstream(ctx context.Context) {
	defer func() {
		w.Stop()
		w.upstream.Close()
	}()

	for {
		if err := w.upstream.SetReadDeadline(time.Now().Add(w.aliveTimeout)); err != nil {
			glog.Errorf("[ServeUpstream] set read deadline for upstream failed: %v", err)
			return
		}

		request, code, err := w.upstream.ReadWireMessage(ctx)
		if err != nil {
			glog.Errorf("[ServeUpstream] read request from upstream failed: %v", err)
			return
		}

		handlerCtx := &Context{
			ctx: ctx,
			Request: &Message{
				Code: code,
				Body: request,
			},
		}

		for _, h := range w.handleFuncChain {
			h(handlerCtx)
			if handlerCtx.Response != nil {
				// the request is processed local
				glog.V(3).Infof("[ServeUpstream] detect response to feedback")
				break
			}
		}

		if handlerCtx.Response != nil {
			if Debug() {
				glog.V(8).Infof("[ServeUpstream] %s", wire.DebugOp(handlerCtx.Response.Body, handlerCtx.Response.Code))
			}
			// write local response
			w.upstream.SetResponseID(w.upstream.RequestID())
			w.upstream.SetRequestID(rand.Int31())
			if err := w.upstream.SetWriteDeadline(time.Now().Add(w.aliveTimeout)); err != nil {
				glog.Errorf("[ServeUpstream] set write deadline for upstream failed: %v", err)
				return
			}
			if err := w.upstream.WriteWireMessage(context.Background(), handlerCtx.Response.Body, handlerCtx.Response.Code); err != nil {
				glog.Errorf("[ServeUpstream] write response to upstream failed: %v", err)
				return
			}
		} else {
			// proxy the request
			if w.downstream == nil {
				glog.Errorf("[ServeUpstream] no downstream connection")
				return
			}
			if Debug() {
				glog.V(8).Infof("[ServeUpstream] %s", wire.DebugOp(handlerCtx.Request.Body, handlerCtx.Request.Code))
			}
			w.downstream.SetRequestID(w.upstream.RequestID())
			w.downstream.SetResponseID(w.upstream.ResponseID())
			if err := w.downstream.SetWriteDeadline(time.Now().Add(w.aliveTimeout)); err != nil {
				glog.Errorf("[ServeUpstream] set write deadline for downstream failed: %v", err)
				return
			}
			if err := w.downstream.WriteWireMessage(context.Background(), handlerCtx.Request.Body, handlerCtx.Request.Code); err != nil {
				glog.Errorf("[ServeUpstream] write request to downstream failed: %v", err)
				return
			}
		}
		if handlerCtx.Quit {
			return
		}
	}
}

// ServeDownstream read response from mongo cluster and proxy to the upstream
func (w *Worker) ServeDownstream(ctx context.Context) {
	defer func() {
		w.Stop()
		w.downstream.Close()
	}()

	for {
		if err := w.downstream.SetReadDeadline(time.Now().Add(w.aliveTimeout)); err != nil {
			glog.Errorf("[ServeDownstream] set read deadline for downstream failed: %v", err)
			return
		}
		response, code, err := w.downstream.ReadWireMessage(context.Background())
		if err != nil {
			glog.Errorf("[ServeDownstream] read downstream failed: %v", err)
			return
		}
		handlerCtx := &Context{
			ctx: ctx,
			Response: &Message{
				Code: code,
				Body: response,
			},
		}
		if Debug() {
			glog.V(8).Infof("[ServeDownstream][PreTransform] %s", wire.DebugOp(handlerCtx.Response.Body, handlerCtx.Response.Code))
		}
		w.TransformResponse(handlerCtx)
		w.upstream.SetRequestID(w.downstream.RequestID())
		w.upstream.SetResponseID(w.downstream.ResponseID())
		if Debug() {
			glog.V(8).Infof("[ServeDownstream][PostTransform] %s", wire.DebugOp(handlerCtx.Response.Body, handlerCtx.Response.Code))
		}
		if err := w.upstream.SetWriteDeadline(time.Now().Add(w.aliveTimeout)); err != nil {
			glog.Errorf("[ServeDownstream] set write deadline for upstream failed: %v", err)
			return
		}
		if err := w.upstream.WriteWireMessage(context.Background(), handlerCtx.Response.Body, handlerCtx.Response.Code); err != nil {
			glog.Errorf("[ServeDownstream] write response to upstream failed: %v", err)
			return
		}
	}
}

func (w *Worker) ConnectBackend(ctx context.Context) error {
	info, err := w.metaClient.Cluster(ctx, w.UID)
	if err != nil {
		glog.Errorf("[Worker][ConnectBackend][%s] get meta failed: %v", w.UID, err)
		return err
	}
	glog.V(5).Infof("[Worker][ConnectBackend][%s] connect backend: %v", w.UID, *info)
	w.targetDB = info.Source()
	client := NewSingleEndpointClient().
		AppName("monog-porxy").
		Cluster("").
		ConnectTimeout(time.Second).
		Source(w.targetDB).
		Password(info.Password()).
		Username(info.Uname())
	if err := client.Connect(info.Endpoint()); err != nil {
		glog.Errorf("[Worker][ConnectBackend][%s] connect to cluster failed: %v", w.UID, err)
		return err
	}
	if err := client.Auth(ctx); err != nil {
		glog.Errorf("[Worker][ConnectBackend][%s] auth on %s failed: %v", w.UID, info.Source(), err)
		return err
	}
	w.downstream = client.conn
	w.clusterTime = client.handshakeInfo.ClusterTime
	go w.ServeDownstream(ctx)
	return nil
}

func ErrorResponse(message string) bsoncore.Document {
	return bsoncore.BuildDocumentFromElements(nil,
		bsoncore.AppendDoubleElement(nil, "ok", 0.0),
		bsoncore.AppendStringElement(nil, "errmsg", message),
	)
}

func ErrorMsg(message string) *wire.MSGInfo {
	return &wire.MSGInfo{
		Flag: 0,
		Kind: wiremessage.SingleDocument,
		Docs: []bsoncore.Document{ErrorResponse(message)},
	}
}

func ErrorReply(message string) *wire.ReplyInfo {
	return &wire.ReplyInfo{
		ReplyFlag: wiremessage.QueryFailure,
		CursorID:  0,
		StartFrom: 0,
		NumReturn: 1,
		Docs:      []bsoncore.Document{ErrorResponse(message)},
	}
}

func CommandName(document bsoncore.Document) (string, error) {
	elements, err := document.Elements()
	if err != nil {
		return "", err
	}
	if len(elements) == 0 {
		return "", fmt.Errorf("cannot get command name")
	}
	return strings.ToLower(elements[0].Key()), nil
}
