package wire

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

var (
	defaultMaxMessageSize uint32 = 48000000
	errResponseTooLarge   error  = errors.New("length of read message too large")
)

type Request struct {
	Header
	Body []byte
}

func (r *Request) GetCommandName() string {
	// skip 4 bytes for document length and 1 byte for element type
	idx := bytes.IndexByte(r.Body[5:], 0x00) // look for the 0 byte after the command name
	return string(r.Body[5 : idx+5])
}

type Response struct {
	Header
	Body []byte
}

// a struct to represent a wire protocol message header.
type Header struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        wiremessage.OpCode
}

// ToWire converts the messageHeader to the wire protocol
func (h *Header) ToWire() []byte {
	var d [16]byte
	b := d[:]
	setInt32(b, 0, h.MessageLength)
	setInt32(b, 4, h.RequestID)
	setInt32(b, 8, h.ResponseTo)
	setInt32(b, 12, int32(h.OpCode))
	return b
}

func (h *Header) WriteTo(w io.Writer) (int64, error) {
	b := h.ToWire()
	n, err := w.Write(b)
	return int64(n), err
}

func ReadHead(r io.Reader) (*Header, error) {
	buffer := make([]byte, 16)
	if _, err := io.ReadFull(r, buffer); err != nil {
		return nil, err
	}

	length, id, to, code, _, ok := wiremessage.ReadHeader(buffer)
	if !ok {
		return nil, errors.New("read head error")
	}
	if length > int32(defaultMaxMessageSize) || length < 16 {
		return nil, errResponseTooLarge
	}
	return &Header{
		MessageLength: length,
		RequestID:     id,
		ResponseTo:    to,
		OpCode:        code,
	}, nil
}

func ReadBody(h *Header, r io.Reader) ([]byte, error) {
	buffer := make([]byte, h.MessageLength-16)
	if _, err := io.ReadFull(r, buffer); err != nil {
		return nil, err
	}
	return buffer, nil
}

// the struct for the 'find' command.
type Find struct {
	RequestID       int32
	Database        string
	Collection      string
	Filter          bson.D
	Sort            bson.D
	Projection      bson.D
	Skip            int32
	Limit           int32
	Tailable        bool
	OplogReplay     bool
	NoCursorTimeout bool
	AwaitData       bool
	Partial         bool
}

// the struct for the 'insert' command
type Insert struct {
	RequestID    int32
	Database     string
	Collection   string
	Documents    []bson.D
	Ordered      bool
	WriteConcern *bson.M
}

type SingleUpdate struct {
	Selector bson.D
	Update   bson.D
	Upsert   bool
	Multi    bool
}

// the struct for the 'update' command
type Update struct {
	RequestID    int32
	Database     string
	Collection   string
	Updates      []SingleUpdate
	Ordered      bool
	WriteConcern *bson.M
}

type SingleDelete struct {
	Selector bson.D
	Limit    int32
}

// struct for 'delete' command
type Delete struct {
	RequestID    int32
	Database     string
	Collection   string
	Deletes      []SingleDelete
	Ordered      bool
	WriteConcern *bson.M
}

// struct for 'getMore' command
type GetMore struct {
	RequestID  int32
	Database   string
	CursorID   int64
	Collection string
	BatchSize  int32
}

func processOpUpdate(body []byte) error {
	fmt.Println("processOpUpdate")
	return nil
}

func processOpInsert(body []byte) error {
	fmt.Println("processOpInsert")
	return nil
}

func DebugOp(body []byte, code wiremessage.OpCode) string {
	switch code {
	case wiremessage.OpQuery:
		return debugOpQuery(body)
	case wiremessage.OpReply:
		return debugOpReply(body)
	case wiremessage.OpMsg:
		return debugOpMsg(body)
	default:
		return ""
	}
}

func debugOpQuery(body []byte) string {
	blocks := []string{"Debug Query:"}
	flag, rem, ok := wiremessage.ReadQueryFlags(body)
	if ok {
		blocks = append(blocks, fmt.Sprintf("flag: %v", flag))
	}
	name, rem, ok := wiremessage.ReadQueryFullCollectionName(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("collection name: %v", name))
	}
	skip, rem, ok := wiremessage.ReadQueryNumberToSkip(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("number to skip: %v", skip))
	}
	nreturn, rem, ok := wiremessage.ReadQueryNumberToReturn(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("number to return: %v", nreturn))
	}
	query, rem, ok := wiremessage.ReadQueryQuery(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("query:\n %v", query.String()))
	}
	selector, rem, ok := wiremessage.ReadQueryReturnFieldsSelector(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("selector:\n %v", selector.String()))
	}
	return strings.Join(blocks, "\n")
}

func debugOpReply(body []byte) string {
	blocks := []string{"Debug Reply:"}
	flag, rem, ok := wiremessage.ReadReplyFlags(body)
	if ok {
		blocks = append(blocks, fmt.Sprintf("flag: %v", flag))
	}
	cursorID, rem, ok := wiremessage.ReadReplyCursorID(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("cursorID: %v", cursorID))
	}
	startFrom, rem, ok := wiremessage.ReadReplyStartingFrom(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("start from: %v", startFrom))
	}
	numreturn, rem, ok := wiremessage.ReadReplyNumberReturned(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("number return: %v", numreturn))
	}
	docs, _, ok := wiremessage.ReadReplyDocuments(rem)
	if ok {
		blocks = append(blocks, "docs:")
		for _, doc := range docs {
			blocks = append(blocks, doc.String())
		}
	}
	return strings.Join(blocks, "\n")
}

func debugOpMsg(body []byte) string {
	blocks := []string{"Debug Msg:"}
	flag, rem, ok := wiremessage.ReadMsgFlags(body)
	if ok {
		blocks = append(blocks, fmt.Sprintf("flag: %v", flag))
	}
	kind, rem, ok := wiremessage.ReadMsgSectionType(rem)
	if ok {
		blocks = append(blocks, fmt.Sprintf("kind: %v", kind))
	}
	switch kind {
	case wiremessage.SingleDocument:
		blocks = append(blocks, "single section:")
		doc, left, ok := wiremessage.ReadMsgSectionSingleDocument(rem)
		if ok {
			blocks = append(blocks, fmt.Sprintf("doc:\n %v", doc.String()))
		}
		if len(left) != 0 {
			_, batchs, _, _ := wiremessage.ReadMsgSectionDocumentSequence(left[1:])
			blocks = append(blocks, "batchs:")
			for _, batch := range batchs {
				blocks = append(blocks, batch.String())
			}
		}
	case wiremessage.DocumentSequence:
		blocks = append(blocks, "section sequence:")
		_, docs, _, ok := wiremessage.ReadMsgSectionDocumentSequence(rem)
		if ok {
			for _, doc := range docs {
				blocks = append(blocks, doc.String())
			}
		}
	}
	return strings.Join(blocks, "\n")
}

func processOpGetMore(body []byte) error {
	fmt.Println("processOpGetMore")
	return nil
}

func processOpDelete(body []byte) error {
	fmt.Println("processOpDelete")
	return nil
}

// all data in the MongoDB wire protocol is little-endian.
// all the read/write functions below are little-endian.
func getInt32(b []byte, pos int) int32 {
	return (int32(b[pos+0])) |
		(int32(b[pos+1]) << 8) |
		(int32(b[pos+2]) << 16) |
		(int32(b[pos+3]) << 24)
}

func setInt32(b []byte, pos int, i int32) {
	b[pos] = byte(i)
	b[pos+1] = byte(i >> 8)
	b[pos+2] = byte(i >> 16)
	b[pos+3] = byte(i >> 24)
}

type ReplyInfo struct {
	ReplyFlag wiremessage.ReplyFlag
	CursorID  int64
	StartFrom int32
	NumReturn int32
	Docs      []bsoncore.Document
}

func DecodeReply(body []byte) (*ReplyInfo, error) {
	info := &ReplyInfo{}

	flag, rem, ok := wiremessage.ReadReplyFlags(body)
	if !ok {
		return nil, errors.New("read reply flag error")
	}
	info.ReplyFlag = flag

	cursorID, rem, ok := wiremessage.ReadReplyCursorID(rem)
	if !ok {
		return nil, errors.New("read cursorID error")
	}
	info.CursorID = cursorID

	startFrom, rem, ok := wiremessage.ReadReplyStartingFrom(rem)
	if !ok {
		return nil, errors.New("read start from error")
	}
	info.StartFrom = startFrom

	numreturn, rem, ok := wiremessage.ReadReplyNumberReturned(rem)
	if !ok {
		return nil, errors.New("read reply numreturn error")
	}
	info.NumReturn = numreturn

	docs, _, ok := wiremessage.ReadReplyDocuments(rem)
	if !ok {
		return nil, errors.New("read reply error")
	}
	info.Docs = append(info.Docs, docs...)
	return info, nil
}

func EncodeReply(info *ReplyInfo) []byte {
	payload := []byte{}
	if info == nil {
		return payload
	}
	payload = wiremessage.AppendReplyFlags(payload, info.ReplyFlag)
	payload = wiremessage.AppendReplyCursorID(payload, info.CursorID)
	payload = wiremessage.AppendReplyStartingFrom(payload, info.StartFrom)
	payload = wiremessage.AppendReplyNumberReturned(payload, info.NumReturn)
	for _, doc := range info.Docs {
		payload = append(payload, doc...)
	}
	return payload
}

type MSGInfo struct {
	Flag wiremessage.MsgFlag
	Kind wiremessage.SectionType
	Docs []bsoncore.Document
	// Ext contains batchs with the necessary information to batch split an operation.
	// This is only used for write oeprations.
	// type Batches struct {
	//     Identifier string 				// key such as "updates" and "deletes"
	//     Documents  []bsoncore.Document   // advanceBatch documents
	//     Current    []bsoncore.Document   // real documents, splited from 'Documents' ,to send
	//     Ordered    *bool
	// }
	// In most cases, batchs do not need to transform, read as MsgSection if necessary.
	// Refer to go-mongo-driver/x/mongo/driver/batches.go for more details.
	Ext []byte
}

func (i *MSGInfo) Document() bsoncore.Document {
	if i.Kind == wiremessage.SingleDocument && len(i.Docs) > 0 {
		return i.Docs[0]
	}
	return nil
}

func (i *MSGInfo) Documents() []bsoncore.Document {
	if i.Kind == wiremessage.DocumentSequence {
		return i.Docs
	}
	return nil
}

func (i *MSGInfo) Batch() []byte {
	return i.Ext
}

func DecodeMSG(body []byte) (*MSGInfo, error) {
	info := &MSGInfo{}

	flag, rem, ok := wiremessage.ReadMsgFlags(body)
	if !ok {
		return nil, errors.New("read message flags failed")
	}
	info.Flag = flag

	kind, rem, ok := wiremessage.ReadMsgSectionType(rem)
	if !ok {
		return nil, errors.New("read message section type failed")
	}
	info.Kind = kind

	switch kind {
	case wiremessage.SingleDocument:
		doc, ext, ok := wiremessage.ReadMsgSectionSingleDocument(rem)
		if !ok {
			return nil, errors.New("read message section single document failed")
		}
		info.Docs = append(info.Docs, doc)
		if len(ext) != 0 {
			info.Ext = ext
		}
	case wiremessage.DocumentSequence:
		// TODO(GODRIVER-617): Implement document sequence returns.
		_, docs, _, ok := wiremessage.ReadMsgSectionDocumentSequence(rem)
		if !ok {
			return nil, errors.New("read message section sequence failed")
		}
		info.Docs = docs
	}
	return info, nil
}

func EncodeMSG(info *MSGInfo) []byte {
	payload := []byte{}
	if info == nil {
		return payload
	}
	payload = wiremessage.AppendMsgFlags(payload, info.Flag)
	// TODO(GODRIVER-617): Implement document sequence returns.
	// Only support SingleDocument currently
	if info.Kind == wiremessage.DocumentSequence {
		return payload
	}
	payload = wiremessage.AppendMsgSectionType(payload, wiremessage.SingleDocument)
	if len(info.Docs) > 0 {
		payload = append(payload, info.Docs[0]...)
	}
	payload = append(payload, info.Ext...)
	return payload
}

type QueryInfo struct {
	Flag       wiremessage.QueryFlag
	Collection string
	Skip       int32
	NumReturn  int32
	Query      bsoncore.Document
	Selector   bsoncore.Document
}

func DecodeQuery(body []byte) (*QueryInfo, error) {
	info := &QueryInfo{}

	flag, rem, ok := wiremessage.ReadQueryFlags(body)
	if !ok {
		return nil, errors.New("read query flag error")
	}
	info.Flag = flag

	name, rem, ok := wiremessage.ReadQueryFullCollectionName(rem)
	if !ok {
		return nil, errors.New("read collection name failed")
	}
	info.Collection = name

	skip, rem, ok := wiremessage.ReadQueryNumberToSkip(rem)
	if !ok {
		return nil, errors.New("read number to skip failed")
	}
	info.Skip = skip

	nreturn, rem, ok := wiremessage.ReadQueryNumberToReturn(rem)
	if !ok {
		return nil, errors.New("read number to return failed")
	}
	info.NumReturn = nreturn

	if nreturn == 0 {
		return info, nil
	}

	query, rem, ok := wiremessage.ReadQueryQuery(rem)
	if !ok {
		return nil, errors.New("read query query failed")
	}
	info.Query = query

	selector, rem, ok := wiremessage.ReadQueryReturnFieldsSelector(rem)
	if ok {
		// no selector, not matter
		info.Selector = selector
	}
	return info, nil
}
