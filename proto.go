package thriftutils

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
)

var _ thrift.TProtocol = &LoggingProtocol{}

type thriftType int

const (
	MESSAGE thriftType = iota
	MAP
	LIST
	SET
	STRUCT
	FIELD
	STRING
	I16
	I32
	I64
	DOUBLE
	UUID
	BINARY
	BYTE
	BOOL
)

type thriftItem struct {
	thriftType thriftType
	name       string
	value      interface{}
}

type LoggingProtocol struct {
	next thrift.TProtocol
	// printer
	w      *bytes.Buffer
	indent bool
	stack  []thriftType
}

func NewLoggingProtocol(wraps thrift.TProtocol) *LoggingProtocol {
	return &LoggingProtocol{
		next:   wraps,
		w:      bytes.NewBuffer(nil),
		indent: true,
	}
}

func (l *LoggingProtocol) indentWriter() {
	if l.indent {
		tabs := ""
		for i := 0; i < len(l.stack); i++ {
			tabs = tabs + "  "
		}
		_, _ = l.w.WriteString(tabs)
	}
}

func (l *LoggingProtocol) write(msg string, args ...interface{}) {
	l.indentWriter()
	_, _ = fmt.Fprintf(l.w, msg, args...)
}

func (l *LoggingProtocol) writeLn(msg string, args ...interface{}) {
	l.write(msg+"\n", args...)
}

func (l *LoggingProtocol) push(item thriftItem) {
	switch item.thriftType {
	case MESSAGE:
		if item.name == "" {
			l.writeLn("message {")
		} else {
			l.writeLn("message (name='%s') {", item.name)
		}
	case STRUCT:
		if item.name == "" {
			l.writeLn("struct {")
		} else {
			l.writeLn("struct (name='%s') {", item.name)
		}
	case FIELD:
		l.write("\"%s\" = ", item.name)
	case MAP:
		l.writeLn("map {")
	case LIST:
		l.writeLn("list [")
	case SET:
		l.writeLn("set [")
	case STRING:
		l.writeLn("\"%s\"", strings.ReplaceAll(item.value.(string), "\"", "\\\""))
	case I16:
		l.writeLn("%d", item.value.(int16))
	case I32:
		l.writeLn("%d", item.value.(int32))
	case I64:
		l.writeLn("%d", item.value.(int64))
	case DOUBLE:
		l.writeLn("%.2f", item.value.(float64))
	case UUID:
		l.writeLn("\"%s\"", item.value.(thrift.Tuuid).String())
	case BINARY:
		l.writeLn("\"%x\"", item.value.([]byte))
	case BYTE:
		l.writeLn("\"%x\"", item.value.(byte))
	case BOOL:
		l.writeLn("%t", item.value.(bool))
	default:
		l.writeLn("UNKNOWN:%+v", item.value)
	}

	// indent parents
	switch item.thriftType {
	case MESSAGE, MAP, LIST, SET, STRUCT, FIELD:
		l.stack = append(l.stack, item.thriftType)
	}
}

func (l *LoggingProtocol) String() string {
	out := l.w.String()
	l.w.Reset()
	return out
}

func (l *LoggingProtocol) pop() {
	if len(l.stack) > 0 {
		head, rest := l.stack[len(l.stack)-1], l.stack[:len(l.stack)-1]
		l.stack = rest
		switch head {
		case MESSAGE, MAP, STRUCT:
			l.writeLn("}")
		case LIST, SET:
			l.writeLn("]")
		}
	}
}

func (l *LoggingProtocol) WriteMessageBegin(ctx context.Context, name string, typeId thrift.TMessageType, seqid int32) error {
	l.push(thriftItem{thriftType: MESSAGE, name: name})
	return l.next.WriteMessageBegin(ctx, name, typeId, seqid)
}

func (l *LoggingProtocol) WriteMessageEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteMessageEnd(ctx)
}

func (l *LoggingProtocol) WriteStructBegin(ctx context.Context, name string) error {
	l.push(thriftItem{thriftType: STRUCT, name: name})
	return l.next.WriteStructBegin(ctx, name)
}

func (l *LoggingProtocol) WriteStructEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteStructEnd(ctx)
}

func (l *LoggingProtocol) WriteFieldBegin(ctx context.Context, name string, typeId thrift.TType, id int16) error {
	l.push(thriftItem{thriftType: FIELD, name: name})
	return l.next.WriteFieldBegin(ctx, name, typeId, id)
}

func (l *LoggingProtocol) WriteFieldEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteFieldEnd(ctx)
}

func (l *LoggingProtocol) WriteFieldStop(ctx context.Context) error {
	return l.next.WriteFieldStop(ctx)
}

func (l *LoggingProtocol) WriteMapBegin(ctx context.Context, keyType thrift.TType, valueType thrift.TType, size int) error {
	l.push(thriftItem{thriftType: MAP})
	return l.next.WriteMapBegin(ctx, keyType, valueType, size)
}

func (l *LoggingProtocol) WriteMapEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteMapEnd(ctx)
}

func (l *LoggingProtocol) WriteListBegin(ctx context.Context, elemType thrift.TType, size int) error {
	l.push(thriftItem{thriftType: LIST})
	return l.next.WriteListBegin(ctx, elemType, size)
}

func (l *LoggingProtocol) WriteListEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteListEnd(ctx)
}

func (l *LoggingProtocol) WriteSetBegin(ctx context.Context, elemType thrift.TType, size int) error {
	l.push(thriftItem{thriftType: SET})
	return l.next.WriteSetBegin(ctx, elemType, size)
}

func (l *LoggingProtocol) WriteSetEnd(ctx context.Context) error {
	l.pop()
	return l.next.WriteSetEnd(ctx)
}

func (l *LoggingProtocol) WriteBool(ctx context.Context, value bool) error {
	l.push(thriftItem{thriftType: BOOL, value: value})
	return l.next.WriteBool(ctx, value)
}

func (l *LoggingProtocol) WriteByte(ctx context.Context, value int8) error {
	l.push(thriftItem{thriftType: BYTE, value: value})
	return l.next.WriteByte(ctx, value)
}

func (l *LoggingProtocol) WriteI16(ctx context.Context, value int16) error {
	l.push(thriftItem{thriftType: I16, value: value})
	return l.next.WriteI16(ctx, value)
}

func (l *LoggingProtocol) WriteI32(ctx context.Context, value int32) error {
	l.push(thriftItem{thriftType: I32, value: value})
	return l.next.WriteI32(ctx, value)
}

func (l *LoggingProtocol) WriteI64(ctx context.Context, value int64) error {
	l.push(thriftItem{thriftType: I64, value: value})
	return l.next.WriteI64(ctx, value)
}

func (l *LoggingProtocol) WriteDouble(ctx context.Context, value float64) error {
	l.push(thriftItem{thriftType: DOUBLE, value: value})
	return l.next.WriteDouble(ctx, value)
}

func (l *LoggingProtocol) WriteString(ctx context.Context, value string) error {
	l.push(thriftItem{thriftType: STRING, value: value})
	return l.next.WriteString(ctx, value)
}

func (l *LoggingProtocol) WriteBinary(ctx context.Context, value []byte) error {
	l.push(thriftItem{thriftType: BINARY, value: value})
	return l.next.WriteBinary(ctx, value)
}

func (l *LoggingProtocol) WriteUUID(ctx context.Context, value thrift.Tuuid) error {
	l.push(thriftItem{thriftType: UUID, value: value})
	return l.next.WriteUUID(ctx, value)
}

// readers!
func (l *LoggingProtocol) ReadMessageBegin(ctx context.Context) (name string, typeId thrift.TMessageType, seqid int32, err error) {
	name, typeId, seqid, err = l.next.ReadMessageBegin(ctx)
	l.push(thriftItem{thriftType: MESSAGE, name: name})
	return
}

func (l *LoggingProtocol) ReadMessageEnd(ctx context.Context) error {
	l.pop()
	return l.next.ReadMessageEnd(ctx)
}

func (l *LoggingProtocol) ReadStructBegin(ctx context.Context) (name string, err error) {
	name, err = l.next.ReadStructBegin(ctx)
	l.push(thriftItem{thriftType: STRUCT, name: name})
	return
}

func (l *LoggingProtocol) ReadStructEnd(ctx context.Context) error {
	l.pop()
	return l.next.ReadStructEnd(ctx)
}

func (l *LoggingProtocol) ReadFieldBegin(ctx context.Context) (name string, typeId thrift.TType, id int16, err error) {
	name, typeId, id, err = l.next.ReadFieldBegin(ctx)
	return
}

func (l *LoggingProtocol) ReadFieldEnd(ctx context.Context) error {
	return l.next.ReadFieldEnd(ctx)
}

func (l *LoggingProtocol) ReadMapBegin(ctx context.Context) (keyType thrift.TType, valueType thrift.TType, size int, err error) {
	keyType, valueType, size, err = l.next.ReadMapBegin(ctx)
	l.push(thriftItem{thriftType: MAP})
	return
}

func (l *LoggingProtocol) ReadMapEnd(ctx context.Context) error {
	l.pop()
	return l.next.ReadMapEnd(ctx)
}

func (l *LoggingProtocol) ReadListBegin(ctx context.Context) (elemType thrift.TType, size int, err error) {
	elemType, size, err = l.next.ReadListBegin(ctx)
	l.push(thriftItem{thriftType: LIST})
	return
}

func (l *LoggingProtocol) ReadListEnd(ctx context.Context) error {
	l.pop()
	return l.next.ReadListEnd(ctx)
}

func (l *LoggingProtocol) ReadSetBegin(ctx context.Context) (elemType thrift.TType, size int, err error) {
	elemType, size, err = l.next.ReadSetBegin(ctx)
	l.push(thriftItem{thriftType: SET})
	return
}

func (l *LoggingProtocol) ReadSetEnd(ctx context.Context) error {
	l.pop()
	return l.next.ReadSetEnd(ctx)
}

func (l *LoggingProtocol) ReadBool(ctx context.Context) (value bool, err error) {
	value, err = l.next.ReadBool(ctx)
	l.push(thriftItem{thriftType: BOOL, value: value})
	return
}

func (l *LoggingProtocol) ReadByte(ctx context.Context) (value int8, err error) {
	value, err = l.next.ReadByte(ctx)
	l.push(thriftItem{thriftType: BYTE, value: value})
	return
}

func (l *LoggingProtocol) ReadI16(ctx context.Context) (value int16, err error) {
	value, err = l.next.ReadI16(ctx)
	l.push(thriftItem{thriftType: I16, value: value})
	return
}

func (l *LoggingProtocol) ReadI32(ctx context.Context) (value int32, err error) {
	value, err = l.next.ReadI32(ctx)
	l.push(thriftItem{thriftType: I32, value: value})
	return
}

func (l *LoggingProtocol) ReadI64(ctx context.Context) (value int64, err error) {
	value, err = l.next.ReadI64(ctx)
	l.push(thriftItem{thriftType: I64, value: value})
	return
}

func (l *LoggingProtocol) ReadDouble(ctx context.Context) (value float64, err error) {
	value, err = l.next.ReadDouble(ctx)
	l.push(thriftItem{thriftType: DOUBLE, value: value})
	return
}

func (l *LoggingProtocol) ReadString(ctx context.Context) (value string, err error) {
	value, err = l.next.ReadString(ctx)
	l.push(thriftItem{thriftType: STRING, value: value})
	return
}

func (l *LoggingProtocol) ReadBinary(ctx context.Context) (value []byte, err error) {
	value, err = l.next.ReadBinary(ctx)
	l.push(thriftItem{thriftType: BINARY, value: value})
	return
}

func (l *LoggingProtocol) ReadUUID(ctx context.Context) (value thrift.Tuuid, err error) {
	value, err = l.next.ReadUUID(ctx)
	l.push(thriftItem{thriftType: UUID, value: value})
	return
}

func (l *LoggingProtocol) Skip(ctx context.Context, fieldType thrift.TType) (err error) {
	return l.next.Skip(ctx, fieldType)
}

func (l *LoggingProtocol) Flush(ctx context.Context) (err error) {
	l.stack = []thriftType{}
	return l.next.Flush(ctx)
}

func (l *LoggingProtocol) Transport() thrift.TTransport {
	return l.next.Transport()
}
