"""Worker plugin of the outbound proxy."""

import temporalio.plugin

from ._config import Options
from ._nexus_handler import call_tool_activity, service_handler
from ._tool_cache import ToolCache


class MCPProxyPlugin(temporalio.plugin.SimplePlugin):
    """Worker plugin that registers the proxy Nexus services and Activities.

    Workers that share the plugin share its tool lists. Factories and
    hooks must support concurrent calls.
    """

    def __init__(self, *, options: Options) -> None:
        """Create the plugin.

        Args:
            options: The proxy configuration.

        Raises:
            ValueError: If there is no upstream, or two upstreams have the same
                service name.
        """
        upstreams = tuple(options.upstreams)
        if not upstreams:
            raise ValueError("Options.upstreams needs at least one Upstream")
        services: set[str] = set()
        for u in upstreams:
            if u.service in services:
                raise ValueError(f"Duplicate Upstream service {u.service!r}")
            services.add(u.service)
        self._caches = {u.service: ToolCache(u) for u in upstreams}
        activities = [call_tool_activity(u) for u in upstreams]
        handlers = [
            service_handler(u, self._caches[u.service], fn)
            for u, fn in zip(upstreams, activities, strict=True)
        ]
        super().__init__(
            "temporalio.nexus_mcp.outbound.MCPProxyPlugin",
            activities=activities,
            nexus_service_handlers=handlers,
        )
