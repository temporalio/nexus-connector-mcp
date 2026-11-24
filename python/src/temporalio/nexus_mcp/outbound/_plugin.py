"""Worker plugin of the outbound proxy."""

import temporalio.plugin

from ._config import Options
from ._nexus_handler import call_tool_activity, service_handler


class MCPProxyPlugin(temporalio.plugin.SimplePlugin):
    """Worker plugin that registers the proxy Nexus services and Activities.

    Factories must support concurrent calls.
    """

    def __init__(self, *, options: Options) -> None:
        """Create the plugin.

        Args:
            options: The proxy configuration.

        Raises:
            ValueError: If there is no upstream, or two upstreams have the same
                service name.
            NotImplementedError: If an upstream sets an option that the proxy
                does not implement yet.
        """
        upstreams = tuple(options.upstreams)
        if not upstreams:
            raise ValueError("Options.upstreams needs at least one Upstream")
        services: set[str] = set()
        for u in upstreams:
            if u.service in services:
                raise ValueError(f"Duplicate Upstream service {u.service!r}")
            services.add(u.service)
        for u in upstreams:
            if u.activity_options_overrides:
                raise NotImplementedError(
                    f"Upstream {u.service!r}: activity_options_overrides is not implemented yet"
                )
            if u.classify_error is not None:
                raise NotImplementedError(
                    f"Upstream {u.service!r}: classify_error is not implemented yet"
                )
        activities = [call_tool_activity(u) for u in upstreams]
        super().__init__(
            "temporalio.nexus_mcp.outbound.MCPProxyPlugin",
            activities=activities,
            nexus_service_handlers=[
                service_handler(u, fn)
                for u, fn in zip(upstreams, activities, strict=True)
            ],
        )
