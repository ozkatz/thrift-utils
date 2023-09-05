package thriftutils

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

var _ thrift.TClient = &RertryingClient{}

var (
	ErrRetriesExhausted = errors.New("retries exhausted")
)

type RetyClientFn func() (thrift.TClient, error)
type ShouldRetryFn func(error) bool
type BackoffHandler interface {
	BackOff(attempt int) bool
}

func RetryOnNetError(err error) bool {
	if texp, ok := err.(thrift.TTransportException); ok {
		if texp.TExceptionType() == thrift.TExceptionTypeTransport {
			return true
		}
	} else if errors.Is(err, syscall.EPIPE) {
		return true
	}
	return false
}

type ExponentialBackoff struct {
	startDuration time.Duration
	maxAttempts   int
	base          float64
}

func NewExponentialBackoff(startDuration time.Duration, maxAttempts int, base float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		startDuration: startDuration,
		maxAttempts:   maxAttempts,
		base:          base,
	}
}

var (
	DefaultExponentialBackoff = NewExponentialBackoff(time.Millisecond*50, 16, 2.0)
)

func (e *ExponentialBackoff) BackOff(attempt int) bool {
	if attempt > e.maxAttempts {
		return false
	}
	attempt += 1
	d := time.Duration(e.startDuration * time.Duration(math.Pow(e.base, float64(attempt))))
	fmt.Printf("attempt = %d, sleeping %dms\n", attempt, d.Milliseconds())
	time.Sleep(d)
	return true
}

type RertryingClient struct {
	retryFn        RetyClientFn
	shouldRetryFn  ShouldRetryFn
	backoffHandler BackoffHandler
	wrappedClient  thrift.TClient
	rwlock         *sync.RWMutex
}

func (c *RertryingClient) handle(attempts int) (bool, error) {
	shouldRetry := c.backoffHandler.BackOff(attempts)
	if !shouldRetry {
		return false, ErrRetriesExhausted
	}
	newWrappedClient, err := c.retryFn()
	if err == nil {
		c.rwlock.Lock()
		c.wrappedClient = newWrappedClient
		c.rwlock.Unlock()
		return true, nil
	}
	// otherwise, we got an error
	return c.shouldRetryFn(err), err
}

// Call implements thrift.TClient.
func (c *RertryingClient) Call(ctx context.Context, method string, args thrift.TStruct, result thrift.TStruct) (thrift.ResponseMeta, error) {
	var (
		err         error
		mt          thrift.ResponseMeta
		attempts    int
		shouldRetry bool = true
	)
	for shouldRetry {
		c.rwlock.RLock()
		mt, err = c.wrappedClient.Call(ctx, method, args, result)
		c.rwlock.RUnlock()
		if err != nil {
			shouldRetry = c.shouldRetryFn(err)
			if shouldRetry {
				attempts++
				shouldRetry, err = c.handle(attempts)
				continue
			} else {
				break
			}
		} else {
			break
		}
	}
	return mt, err
}

func NewRertryingClient(fn RetyClientFn, shoudRetryFn ShouldRetryFn, backoffHandler BackoffHandler) (*RertryingClient, error) {
	wrappedClient, err := fn()
	if err != nil {
		return nil, err
	}
	return &RertryingClient{
		wrappedClient:  wrappedClient,
		retryFn:        fn,
		backoffHandler: backoffHandler,
		shouldRetryFn:  shoudRetryFn,
		rwlock:         &sync.RWMutex{},
	}, nil
}
