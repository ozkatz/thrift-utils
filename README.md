# Thrift utils

This is a small wrapper library around a `thrift.TProcessor` that emits a textual representation of Thrift function calls, their input and output.
Additionally, it provides a small TClient that allows retrying on network errors

## Installing

```bash
go get github.com/ozkatz/thrift-utils
```

## Using (logging wrapper)

```go
import (
    "github.com/apache/thrift/lib/go/thrift"
    tutil "github.com/ozkatz/thrift-utils"
    ...
)

// existing processor
processor := hello_world.NewHelloWorldProcesor(myHelloWorldHandler{})

// Let's wrap it with a proxy:
processor = tutil.Log(processor, func(c *tutil.Call) {
    logLine := logger.WithFields(logging.Fields{
        "name": c.Name,
        "input": c.Input,
        "output": c.Output,
        "took_ms": c.Took.Milliseconds(),
    })

    if c.Err != nil {
        logLine.WithError(c.Err).Error("error while running thrift call")
    } else {
        logLine.Debug("thrift call executed successfully")
    }   
})

// and use it as normal:
server := thrift.NewTSimpleServer4(proc, transport, transportFactory, protocolFactory)
server.Serve()

```

## Using (client wrapper that retries on socket errors)

This is only useful because I want to restart the upstream server from time to time and the TCP connection gets broken.

```go
import (
    "github.com/apache/thrift/lib/go/thrift"
    tutil "github.com/ozkatz/thrift-utils"
    ...
)


func connectUpstream(addr string) (*hello_world.ThriftHelloWorldClient, error) {
	connectFn := func() (thrift.TClient, error) {
		cfg := &thrift.TConfiguration{}
		transport := thrift.NewTSocketConf(addr, cfg)
		if err := transport.Open(); err != nil {
			return nil, err
		}
		protocolFactory := thrift.NewTBinaryProtocolFactoryConf(cfg)
		return thrift.NewTStandardClient(
			protocolFactory.GetProtocol(transport),
			protocolFactory.GetProtocol(transport)), nil
	}
    
	client, err := tutil.NewRertryingClient(
		connectFn, 
        tutil.RetryOnNetError,
        tutil.DefaultExponentialBackoff)
	if err != nil {
		return nil, err
	}
	return hello_world.NewThriftHelloWorldClient(client), nil
}

func RunProxyServer(ctx context.Context, upstream, addr string) error {
	logging.SetLevel("debug")
	logging.SetOutputs([]string{"-"}, 0, 0)
	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(nil)
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}

	client, err := connectUpstream(upstream)
	if err != nil {
		return err
	}

	processor := hello_world.NewThriftHelloWorldProcessor(client)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	logging.FromContext(ctx).WithFields(logging.Fields{"addr": addr}).Info("starting Thrift proxy server")
	return server.Serve()
}

```
