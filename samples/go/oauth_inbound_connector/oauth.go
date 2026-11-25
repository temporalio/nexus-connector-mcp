package main

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

const (
	// protectedResourceMetadataPath starts the path of the protected resource
	// metadata (RFC 9728). The resource path follows it.
	protectedResourceMetadataPath = "/.well-known/oauth-protected-resource"
)

var (
	resource = flag.String("resource", "", "public MCP URL, such as https://connector.example.com/mcp")
	issuer   = flag.String("issuer", "", "trusted token issuer")
	audience = flag.String("audience", "", "API audience in the token aud claim")
	scope    = flag.String("scope", "", "required token scope, such as mcp:access (optional)")
)

// withOAuth protects handler with bearer-token checks and serves the protected
// resource metadata:
//   - go-oidc does issuer discovery, signature, issuer, audience, and expiry
//     checks, and caches the signing keys.
//   - The MCP SDK auth package reads the bearer token, checks scopes, returns
//     401 challenges, and serves the protected resource metadata.
//
// The path of the resource URL routes to handler. Raw tokens do not enter
// Temporal.
func withOAuth(handler http.Handler) (http.Handler, error) {
	resourceURL, err := url.Parse(*resource)
	if err != nil {
		return nil, err
	}
	// The provider reads the issuer discovery document and fetches the signing
	// keys.
	provider, err := oidc.NewProvider(context.Background(), *issuer)
	if err != nil {
		return nil, err
	}
	// ClientID is the API audience, not the client ID of the MCP application.
	verifier := provider.Verifier(&oidc.Config{ClientID: *audience})
	verify := func(ctx context.Context, raw string, _ *http.Request) (*auth.TokenInfo, error) {
		token, err := verifier.Verify(ctx, raw)
		if err != nil || token.Subject == "" {
			return nil, auth.ErrInvalidToken
		}
		var claims struct {
			Scope string `json:"scope"`
		}
		if err := token.Claims(&claims); err != nil {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{
			// The connector reads UserID for task ownership checks.
			UserID:     token.Issuer + "#" + token.Subject,
			Expiration: token.Expiry,
			Scopes:     strings.Fields(claims.Scope),
		}, nil
	}

	var scopes []string
	if *scope != "" {
		scopes = []string{*scope}
	}
	metadataURL := resourceURL.ResolveReference(&url.URL{Path: protectedResourceMetadataPath + resourceURL.Path})
	mux := http.NewServeMux()
	mux.Handle(metadataURL.Path, auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource:             *resource,
		AuthorizationServers: []string{*issuer},
		ScopesSupported:      scopes,
	}))
	mux.Handle(resourceURL.Path, auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{
		ResourceMetadataURL: metadataURL.String(),
		Scopes:              scopes,
	})(handler))
	return mux, nil
}
