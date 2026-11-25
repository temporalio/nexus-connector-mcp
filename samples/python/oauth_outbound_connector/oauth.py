"""OAuth for the sample Worker."""

import argparse
import os

import httpx2
from mcp.client.auth.extensions.client_credentials import ClientCredentialsOAuthProvider
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken


def add_oauth_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the OAuth arguments to parser."""
    parser.add_argument(
        "--issuer", required=True, help="OAuth authorization server issuer"
    )
    parser.add_argument(
        "--client-id",
        required=True,
        help="OAuth client ID. OAUTH_CLIENT_SECRET holds the secret.",
    )
    parser.add_argument("--scope", help="requested scopes, space separated (optional)")


def oauth_http_client(args: argparse.Namespace, resource: str) -> httpx2.AsyncClient:
    """Return an HTTP client that adds an OAuth access token to each request.

    It uses the client credentials grant, because the Worker has no user to
    log in. The provider discovers the token endpoint from the protected
    resource metadata of resource (RFC 9728). It only sends the credentials to
    the authorization server of issuer.

    The provider caches the token and gets a new one when it expires. It holds
    a lock, so all tool calls share it.

    The secret comes from the environment, so it stays out of the process
    arguments. Tokens and the secret do not enter Temporal.
    """
    provider = ClientCredentialsOAuthProvider(
        server_url=resource,
        storage=_MemoryTokenStorage(),
        client_id=args.client_id,
        client_secret=os.environ["OAUTH_CLIENT_SECRET"],
        scope=args.scope,
        issuer=args.issuer,
    )
    # These are the MCP SDK default timeouts. The httpx default of 5 seconds
    # is too short for a long tool call.
    return httpx2.AsyncClient(auth=provider, timeout=httpx2.Timeout(30.0, read=300.0))


class _MemoryTokenStorage:
    """Keeps the token in memory. The provider needs a TokenStorage."""

    def __init__(self) -> None:
        self._tokens: OAuthToken | None = None
        self._client_info: OAuthClientInformationFull | None = None

    async def get_tokens(self) -> OAuthToken | None:
        return self._tokens

    async def set_tokens(self, tokens: OAuthToken) -> None:
        self._tokens = tokens

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        return self._client_info

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        self._client_info = client_info
