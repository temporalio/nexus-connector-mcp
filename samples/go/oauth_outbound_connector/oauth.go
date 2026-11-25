package main

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/oauth2/clientcredentials"
)

var (
	tokenURL = flag.String("token-url", "", "OAuth token endpoint")
	clientID = flag.String("client-id", "", "OAuth client ID. OAUTH_CLIENT_SECRET holds the secret.")
	scope    = flag.String("scope", "", "requested scopes, space separated (optional)")
)

// oauthHTTPClient returns an HTTP client that adds an OAuth access token to
// each request. It uses the client credentials grant, because the Worker has
// no user to log in.
//
// The client caches the token and gets a new one before it expires. It is safe
// for concurrent use, so all tool calls share it. The resource parameter
// (RFC 8707) asks for a token for the upstream URL.
//
// The secret comes from the environment, so it stays out of the process
// arguments. Tokens and the secret do not enter Temporal.
func oauthHTTPClient(resource string) *http.Client {
	config := clientcredentials.Config{
		ClientID:       *clientID,
		ClientSecret:   os.Getenv("OAUTH_CLIENT_SECRET"),
		TokenURL:       *tokenURL,
		Scopes:         strings.Fields(*scope),
		EndpointParams: url.Values{"resource": {resource}},
	}
	return config.Client(context.Background())
}
