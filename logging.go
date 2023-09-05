package thriftutils

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type Call struct {
	Name     string
	Input    string
	Output   string
	Duration time.Duration
	Err      thrift.TException
}

type EmitterFn func(c *Call)

func StdoutLogger(c *Call) {
	if c.Err != nil {
		fmt.Printf("[ERROR] calling: %s\nInput: %s\nOutput: %s\nError: %s\n(took: %dms)\n\n\n",
			c.Name, c.Input, c.Output, c.Err.Error(), c.Duration.Milliseconds())
		return
	}
	fmt.Printf("[INFO] calling: %s\nInput: %s\nOutput: %s\n(took: %dms)\n\n\n",
		c.Name, c.Input, c.Output, c.Duration.Milliseconds())
}
