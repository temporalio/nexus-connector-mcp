package inbound

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// idPrefix starts every operation ID that the connector generates.
	idPrefix = "mcp-inbound"
	// maxIDPrefixBytes limits the variable part of an operation ID. Service,
	// endpoint, and operation names have no length limit of their own.
	maxIDPrefixBytes = 800
	// maxOperationIDBytes limits an operation ID that a client sends, before
	// any Temporal call.
	maxOperationIDBytes = 1024
	// requestIDBytes is the size of the random suffix of an operation ID.
	requestIDBytes = 16
	// statelessProtocolVersion is the first MCP version without an
	// initialize handshake. Protocol versions are dates, so string order is
	// date order.
	//
	// TODO(long-nt-tran): use the SDK constant when the SDK exports it. SDK
	// v1.8.0 keeps its protocol version constants unexported.
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
	var b [requestIDBytes]byte
	_, _ = rand.Read(b[:])
	if len(prefix) > maxIDPrefixBytes {
		prefix = prefix[:maxIDPrefixBytes-100] + fmt.Sprintf("-%x", sha256.Sum256([]byte(prefix)))
	}
	if owner != "" {
		prefix += "-" + owner
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// errInvalidOperationID reports an ID that the connector did not generate
// for the caller.
var errInvalidOperationID = errors.New("invalid operation ID")

// checkOperationID checks an operation ID that a client sends, before any
// Temporal call. The ID must have the format of newOperationID: the
// connector prefix and a random suffix in hex. With a verified caller, the
// owner hash before the suffix must match that caller. Without a verified
// caller, any ID of the right format passes.
func checkOperationID(id, owner string) error {
	separator := strings.LastIndexByte(id, '-')
	if !strings.HasPrefix(id, idPrefix+"-") || separator <= 0 || len(id) > maxOperationIDBytes || len(id)-separator-1 != 2*requestIDBytes {
		return errInvalidOperationID
	}
	if _, err := hex.DecodeString(id[separator+1:]); err != nil {
		return errInvalidOperationID
	}
	if owner != "" {
		rest := id[:separator]
		separator = strings.LastIndexByte(rest, '-')
		if separator <= 0 || rest[separator+1:] != owner {
			return errInvalidOperationID
		}
	}
	return nil
}
