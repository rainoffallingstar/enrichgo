package netutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	HTTPClient   *http.Client
	Timeout      time.Duration
	MaxAttempts  int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	UserAgent    string
	Sleep        func(time.Duration)
	RetryOnError func(error) bool
}

// Client is an http client wrapper with sane defaults (timeouts + retries for transient failures).
// It implements the common `Do(*http.Request)` interface used in this repo.
type Client struct {
	hc          *http.Client
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	userAgent   string
	sleep       func(time.Duration)
	retryErr    func(error) bool
}

func NewClient(opts Options) *Client {
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	if opts.Timeout > 0 {
		hc.Timeout = opts.Timeout
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 4
	}
	baseBackoff := opts.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = 500 * time.Millisecond
	}
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 8 * time.Second
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	retryErr := opts.RetryOnError
	if retryErr == nil {
		retryErr = func(err error) bool {
			// http.Client already wraps a lot of network errors; keep it simple and retry on any non-context error.
			return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
		}
	}

	ua := opts.UserAgent
	if ua == "" {
		ua = "enrichgo"
	}
	return &Client{
		hc:          hc,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
		userAgent:   ua,
		sleep:       sleep,
		retryErr:    retryErr,
	}
}

var defaultClient = NewClient(Options{Timeout: 60 * time.Second})

func DefaultClient() *Client { return defaultClient }

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	if c == nil || c.hc == nil {
		return nil, fmt.Errorf("nil client")
	}

	retryableMethod := req.Method == http.MethodGet || req.Method == http.MethodHead || req.Method == http.MethodOptions
	canReplayBody := req.Body == nil || req.GetBody != nil

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		attemptReq := req.Clone(req.Context())
		if req.Body != nil {
			if req.GetBody == nil {
				// Non-replayable body: only the first attempt is safe.
				if attempt > 1 {
					return nil, fmt.Errorf("request body is not replayable (missing GetBody)")
				}
			} else {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				attemptReq.Body = body
			}
		}
		if attemptReq.Header.Get("User-Agent") == "" && c.userAgent != "" {
			attemptReq.Header.Set("User-Agent", c.userAgent)
		}

		resp, err := c.hc.Do(attemptReq)
		if err == nil && resp != nil && resp.StatusCode < 400 {
			return resp, nil
		}

		shouldRetry := false
		retryAfter := time.Duration(0)

		if err != nil {
			shouldRetry = c.retryErr(err)
		} else if resp != nil {
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500 {
				shouldRetry = true
				retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			}
		}

		if attempt >= c.maxAttempts || !shouldRetry || (!retryableMethod && !canReplayBody) {
			if err != nil {
				return nil, err
			}
			return resp, nil
		}

		if resp != nil && resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}

		delay := backoffDelay(attempt, c.baseBackoff, c.maxBackoff)
		if retryAfter > 0 && retryAfter < delay {
			delay = retryAfter
		}
		c.sleep(delay)
	}

	return nil, fmt.Errorf("unreachable")
}

func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	// attempt is 1-based.
	pow := 1 << (attempt - 1)
	d := time.Duration(pow) * base
	if d > max {
		d = max
	}
	// Add small jitter to reduce thundering herd.
	jitter := time.Duration(rand.Int63n(int64(d/5 + 1)))
	return d - (d / 10) + jitter
}
