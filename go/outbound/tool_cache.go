package outbound

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/log"
)

const (
	// minDiscoveryBackoff and maxDiscoveryBackoff bound the backoff
	// after a failed discovery.
	minDiscoveryBackoff = time.Second
	maxDiscoveryBackoff = time.Minute
)

var (
	// errToolListUnavailable means that discovery never succeeded.
	errToolListUnavailable = errors.New("upstream tool list is not available")
	// errToolNotFound means that the upstream tool list has no tool with the
	// name.
	errToolNotFound = errors.New("unknown upstream tool")
)

// toolCache holds the last complete tool list of one upstream. Each list_tools
// call refreshes it, and a tool call refreshes it on a cache miss. When
// discovery fails, it serves the last successful list. A published list is
// never changed, so readers share it without copies.
type toolCache struct {
	upstream *Upstream

	// Using RWMutex here, since this is probably a read-heavy cache.
	mu sync.RWMutex

	// tools is nil until discovery succeeds.
	// The reason why we keep this list separate from the map nameToTools
	// is because Go map doesn't preserve ordering. If the upstream provides
	// the list of tools in some order, for now we want to stably preserve it,
	// hence using this tools list.
	tools []*mcp.Tool
	// nameToTools maps each tool name to its entry in tools.
	nameToTools     map[string]*mcp.Tool
	nextDiscoveryAt time.Time
	backoff         time.Duration
}

func newToolCache(u *Upstream) *toolCache {
	return &toolCache{upstream: u}
}

// refresh reads all tool-list pages of the upstream. It replaces the tool
// list only when discovery succeeds. After a failure, it backs off with
// jitter, and calls during the backoff return without discovery. The
// caller's context sets the deadline. Concurrent callers run their own
// discovery, and the last one to finish sets the tool list.
//
// TODO(long-nt-tran): use singleflight to share 1 listTools call across
// callers. Need to be careful which calls we can optimize away and which
// we always wanna reach upstream for.
func (c *toolCache) refresh(ctx context.Context) (err error) {
	c.mu.RLock()
	nextDiscoveryAt := c.nextDiscoveryAt
	c.mu.RUnlock()
	if time.Now().Before(nextDiscoveryAt) {
		return nil
	}
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch {
		case err == nil:
			c.nextDiscoveryAt, c.backoff = time.Time{}, 0
		case ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded):
			// A cancelled caller does not start a backoff. A failure or a
			// discovery timeout does.
			var wait time.Duration
			c.backoff, wait = nextBackoff(c.backoff)
			c.nextDiscoveryAt = time.Now().Add(wait)
		}
	}()

	tools, err := listTools(ctx, c.upstream)
	if err != nil {
		return err
	}
	nameToTools := make(map[string]*mcp.Tool, len(tools))
	for _, tool := range tools {
		nameToTools[tool.Name] = tool
	}
	c.mu.Lock()
	c.tools, c.nameToTools = tools, nameToTools
	c.mu.Unlock()
	return nil
}

// nextBackoff implements exponential backoff with jitter for discovery
// retries. It avoids a retry storm when an upstream is down: each Worker
// skips discovery for a while after a failure, and the random wait keeps
// Workers from retrying at the same moment.
//
// backoff doubles from the previous backoff, from minDiscoveryBackoff up to
// maxDiscoveryBackoff. wait is a random duration in [backoff/2, backoff].
func nextBackoff(previous time.Duration) (backoff, wait time.Duration) {
	backoff = min(max(minDiscoveryBackoff, previous*2), maxDiscoveryBackoff)
	wait = backoff/2 + rand.N(backoff/2+1)
	return backoff, wait
}

// refreshAndLog runs refresh and logs a failure. The log leaves out the
// error, because upstream errors can hold credentials and private URLs.
func (c *toolCache) refreshAndLog(ctx context.Context, logger log.Logger) {
	if err := c.refresh(ctx); err != nil && ctx.Err() == nil {
		logger.Warn("could not discover upstream MCP tools; using the last successful tool list", "service", c.upstream.Service)
	}
}

// listTools runs discovery and returns the last successful tool list. If
// discovery never succeeded, it returns errToolListUnavailable.
func (c *toolCache) listTools(ctx context.Context, logger log.Logger) ([]*mcp.Tool, error) {
	c.refreshAndLog(ctx, logger)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.tools == nil {
		return nil, errToolListUnavailable
	}
	return c.tools, nil
}

// findTool returns the named tool. A cache hit makes no upstream call. A cache
// miss refreshes the cache once. If discovery never succeeded, it returns
// errToolListUnavailable. If the upstream has no such tool, it returns
// errToolNotFound.
func (c *toolCache) findTool(ctx context.Context, logger log.Logger, name string) (*mcp.Tool, error) {
	c.mu.RLock()
	tool := c.nameToTools[name]
	c.mu.RUnlock()
	if tool != nil {
		return tool, nil
	}

	c.refreshAndLog(ctx, logger)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.tools == nil {
		return nil, errToolListUnavailable
	}
	if tool = c.nameToTools[name]; tool == nil {
		return nil, errToolNotFound
	}
	return tool, nil
}
