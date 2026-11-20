package outbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
)

const (
	// minDiscoveryRetryDelay and maxDiscoveryRetryDelay bound the backoff
	// after a failed discovery.
	minDiscoveryRetryDelay = time.Second
	maxDiscoveryRetryDelay = time.Minute
)

// catalog holds the last complete tool list of one upstream. A published list
// is never changed, so readers share it without copies.
type catalog struct {
	upstream Upstream
	// refreshLock lets one discovery run at a time. It is a channel, so a
	// waiting caller can stop when its context ends.
	refreshLock chan struct{}

	mu         sync.RWMutex
	tools      []*mcp.Tool
	discovered bool
	retryAt    time.Time
	retryDelay time.Duration
}

func newCatalog(u Upstream) *catalog {
	return &catalog{upstream: u, refreshLock: make(chan struct{}, 1)}
}

// refresh reads all tool-list pages of the upstream. It replaces the tool
// list only when discovery succeeds. After a failure, it backs off with
// jitter, and calls during the backoff return without discovery.
func (c *catalog) refresh(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	select {
	case c.refreshLock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.refreshLock }()

	c.mu.RLock()
	retryAt := c.retryAt
	c.mu.RUnlock()
	if time.Now().Before(retryAt) {
		return nil
	}
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch {
		case err == nil:
			c.retryAt, c.retryDelay = time.Time{}, 0
		case ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded):
			// A cancelled caller does not start a backoff. A failure or a
			// discovery timeout does.
			c.retryDelay = min(max(minDiscoveryRetryDelay, c.retryDelay*2), maxDiscoveryRetryDelay)
			jitter := time.Duration(rand.Int64N(int64(c.retryDelay/2) + 1))
			c.retryAt = time.Now().Add(c.retryDelay/2 + jitter)
		}
	}()

	session, err := c.upstream.ClientFactory(ctx)
	if err != nil {
		return err
	}
	if session == nil {
		return errors.New("ClientFactory returned no session")
	}
	defer func() { _ = session.Close() }()
	tools := []*mcp.Tool{}
	names := map[string]bool{}
	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			return err
		}
		if tool == nil || !isToolName(tool.Name) {
			return errors.New("upstream returned an empty or reserved tool name")
		}
		if names[tool.Name] {
			return fmt.Errorf("upstream returned tool %q more than once", tool.Name)
		}
		names[tool.Name] = true
		tools = append(tools, tool)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.tools, c.discovered = tools, true
	c.mu.Unlock()
	return nil
}

// refreshAndLog runs refresh and logs a failure. The log leaves out the
// error, because upstream errors can hold credentials and private URLs.
func (c *catalog) refreshAndLog(ctx context.Context) {
	if err := c.refresh(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("could not discover upstream MCP tools; using the last successful tool list", "service", c.upstream.Service)
	}
}

// listTools runs discovery and returns the last successful tool list. If
// discovery never succeeded, it returns an unavailable handler error.
func (c *catalog) listTools(ctx context.Context) ([]*mcp.Tool, error) {
	c.refreshAndLog(ctx)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.discovered {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool list is not available")
	}
	return c.tools, nil
}

// lookup returns the named tool, or nil if it is absent. discovered is false
// if discovery never succeeded.
func (c *catalog) lookup(name string) (tool *mcp.Tool, discovered bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, candidate := range c.tools {
		if candidate.Name == name {
			return candidate, true
		}
	}
	return nil, c.discovered
}
