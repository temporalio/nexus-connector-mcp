package inbound

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// idPrefix starts every operation ID that the connector generates.
	idPrefix = "mcp-inbound"
	// maxIDPrefixBytes limits the variable part of an operation ID. Service,
	// endpoint, and operation names have no length limit of their own.
	maxIDPrefixBytes = 800
	// statelessProtocolVersion is the first MCP version without an
	// initialize handshake. Protocol versions are dates, so string order is
	// date order.
	statelessProtocolVersion = "2026-07-28"
)

type requestExtraKey struct{}

// requestExtra returns the MCP SDK request extra of the current request, or
// nil. It holds the verified token info and the HTTP headers.
func requestExtra(ctx context.Context) *mcp.RequestExtra {
	extra, _ := ctx.Value(requestExtraKey{}).(*mcp.RequestExtra)
	return extra
}

// tokenInfoIdentity is the default Options.CallerIdentity. It returns the
// user ID of the MCP SDK token info. Token verifiers set it to issuer#subject.
func tokenInfoIdentity(ctx context.Context) (string, bool) {
	extra := requestExtra(ctx)
	if extra == nil || extra.TokenInfo == nil || extra.TokenInfo.UserID == "" {
		return "", false
	}
	return extra.TokenInfo.UserID, true
}

// ownerHash returns the hex SHA-256 of a verified caller identity.
func ownerHash(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

// operationIDPrefix returns "mcp-inbound-{transport}[-{stateful|stateless}]".
// A visibility query such as OperationId STARTS_WITH
// "mcp-inbound-http-stateless-" then finds the calls of one transport and
// protocol mode. The mode is absent when the session has no protocol version.
func operationIDPrefix(req mcp.Request) string {
	prefix := idPrefix + "-stdio"
	if extra := req.GetExtra(); extra != nil && extra.Header != nil {
		prefix = idPrefix + "-http"
	}
	ss, ok := req.GetSession().(*mcp.ServerSession)
	if !ok || ss == nil || ss.InitializeParams() == nil || ss.InitializeParams().ProtocolVersion == "" {
		return prefix
	}
	if ss.InitializeParams().ProtocolVersion >= statelessProtocolVersion {
		return prefix + "-stateless"
	}
	return prefix + "-stateful"
}

// newOperationID returns "{prefix}-{owner}-{request_id}". request_id is 128
// random bits in hex. Without a verified caller, owner is absent. A prefix
// longer than maxIDPrefixBytes is cut and gets the SHA-256 of the full
// prefix, so the ID stays within the server ID length limit and stays unique.
func newOperationID(prefix, owner string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	if len(prefix) > maxIDPrefixBytes {
		prefix = prefix[:maxIDPrefixBytes-100] + fmt.Sprintf("-%x", sha256.Sum256([]byte(prefix)))
	}
	if owner != "" {
		prefix += "-" + owner
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}
