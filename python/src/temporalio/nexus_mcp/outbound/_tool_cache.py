"""Last successful tool list of an upstream."""

import random
import time
from typing import Any

from temporalio import nexus

from ._config import Upstream
from ._mcp_client import list_tools

# _MIN_DISCOVERY_BACKOFF and _MAX_DISCOVERY_BACKOFF bound the backoff, in
# seconds, after a failed discovery.
_MIN_DISCOVERY_BACKOFF = 1.0
_MAX_DISCOVERY_BACKOFF = 60.0


class ToolListUnavailableError(Exception):
    """Discovery never succeeded."""

    def __init__(self) -> None:
        """Create the error."""
        super().__init__("upstream tool list is not available")


class ToolNotFoundError(Exception):
    """The upstream tool list has no tool with the name."""

    def __init__(self) -> None:
        """Create the error."""
        super().__init__("unknown upstream tool")


class ToolCache:
    """The last complete tool list of one upstream.

    Each list_tools call refreshes it, and a tool call refreshes it on a cache
    miss. When discovery fails, it serves the last successful list. A
    published list is never changed, so readers share it without copies.
    """

    def __init__(self, upstream: Upstream) -> None:
        """Create an empty cache for upstream."""
        self.upstream = upstream
        # None until discovery succeeds. A successful discovery always stores a
        # dict, also when the upstream has no tools. A dict keeps insertion
        # order, so list_tools returns the tools in the same order as the
        # upstream.
        self.tools: dict[str, dict[str, Any]] | None = None
        self._next_discovery_at = 0.0
        self._backoff = 0.0

    async def refresh(self) -> None:
        """Read all tool-list pages of the upstream.

        It replaces the tool list only when discovery succeeds. After a
        failure, it backs off with jitter, and calls during the backoff return
        without discovery. The caller sets the deadline. Concurrent callers run
        their own discovery, and the last one to finish sets the tool list.
        """
        # TODO(long-nt-tran): share 1 list_tools call across callers, like Go's
        # singleflight. Need to be careful which calls we can optimize away and
        # which we always wanna reach upstream for.
        if time.monotonic() < self._next_discovery_at:
            return
        try:
            tools = await list_tools(self.upstream)
        except Exception:
            self._backoff, wait = _next_backoff(self._backoff)
            self._next_discovery_at = time.monotonic() + wait
            raise
        self.tools = tools
        self._next_discovery_at = self._backoff = 0.0

    async def refresh_and_log(self) -> None:
        """Run refresh and log a failure.

        The log leaves out the error, because upstream errors can hold
        credentials and private URLs.
        """
        try:
            await self.refresh()
        # Any upstream error keeps the last successful tool list.
        except Exception:  # noqa: BLE001
            # The Temporal Nexus logger adds the service and operation to the
            # log.
            nexus.logger.warning(
                "Could not discover upstream MCP tools for service %s; using the last successful tool list",
                self.upstream.service,
            )

    async def list_tools(self) -> list[dict[str, Any]]:
        """Run discovery and return the last successful tool list.

        Raises:
            ToolListUnavailableError: If discovery never succeeded.
        """
        await self.refresh_and_log()
        if self.tools is None:
            raise ToolListUnavailableError()
        return list(self.tools.values())

    async def find_tool(self, name: str) -> dict[str, Any]:
        """Return the named tool.

        A cache hit makes no upstream call. A cache miss refreshes the cache
        once.

        Raises:
            ToolListUnavailableError: If discovery never succeeded.
            ToolNotFoundError: If the upstream has no such tool.
        """
        if self.tools is not None and name in self.tools:
            return self.tools[name]
        await self.refresh_and_log()
        if self.tools is None:
            raise ToolListUnavailableError()
        if name not in self.tools:
            raise ToolNotFoundError()
        return self.tools[name]


def _next_backoff(previous: float) -> tuple[float, float]:
    """Implement exponential backoff with jitter for discovery retries.

    It avoids a retry storm when an upstream is down: each Worker skips
    discovery for a while after a failure, and the random wait keeps Workers
    from retrying at the same moment.

    The backoff doubles from the previous backoff, from _MIN_DISCOVERY_BACKOFF
    up to _MAX_DISCOVERY_BACKOFF. The wait is a random duration in
    [backoff/2, backoff]. Both are in seconds.
    """
    backoff = min(max(_MIN_DISCOVERY_BACKOFF, previous * 2), _MAX_DISCOVERY_BACKOFF)
    return backoff, random.uniform(backoff / 2, backoff)
