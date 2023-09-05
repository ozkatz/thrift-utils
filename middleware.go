package thriftutils

import (
	"context"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type loggingMiddleware struct {
	emit EmitterFn
	name string
	next thrift.TProcessorFunction
}

func (l *loggingMiddleware) Process(ctx context.Context, seqId int32, in, out thrift.TProtocol) (bool, thrift.TException) {
	inLogger := NewLoggingProtocol(in)
	outLogger := NewLoggingProtocol(out)

	before := time.Now()
	outBool, outErr := l.next.Process(ctx, seqId, inLogger, outLogger)
	took := time.Since(before)

	call := &Call{
		Name:     l.name,
		Input:    inLogger.String(),
		Output:   outLogger.String(),
		Duration: took,
	}

	l.emit(call)
	return outBool, outErr
}
