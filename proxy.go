package thriftutils

import "github.com/apache/thrift/lib/go/thrift"

func Log(processor thrift.TProcessor, emitter EmitterFn) thrift.TProcessor {
	if emitter == nil {
		emitter = StdoutLogger
	}
	return thrift.WrapProcessor(processor,
		func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
			return &loggingMiddleware{
				emit: emitter,
				name: name,
				next: next,
			}
		})
}
