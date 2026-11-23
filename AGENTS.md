# AGENTS.md

## Git Workflow

### Branch and PR Requirements

- **NEVER** push changes directly to the `main` branch.
- **ALWAYS** create a new branch for changes.
- **ALWAYS** submit a Pull Request for code review.
- Branch naming convention: `username/feature-name`.

### Commit Requirements

- When possible, use `[pkg]: <what changed>` for the first line of the commit message. `pkg` is the
  package or area, for example `[go/inbound]`, `[python]`, `[typescript]`, or `[ci]`.
- Do not add co-author attribution for AI agents.
- Run `mise run format` before committing.

## Repository Layout

| Directory     | Contents                                                                   |
| ------------- | -------------------------------------------------------------------------- |
| `go/`         | Go module `github.com/temporalio/nexus-connector-mcp/go`.                  |
| `go/inbound`  | `NewServer`: returns an MCP server that maps MCP requests to Nexus.        |
| `go/outbound` | `NewPlugin`: Worker plugin that proxies upstream MCP servers.              |
| `go/handler`  | `Tool`, `Register`: mark Nexus operations as MCP tools.                    |
| `python/`     | `temporalio-nexus-mcp`. Source is in `python/src/temporalio/nexus_mcp`.    |
| `typescript/` | `@temporalio/nexus-mcp`. Source is in `typescript/src`.                    |

Each language directory is a separate package with its own lockfile. The root `mise.toml` pins the
tools and defines the tasks.

The API shapes follow the inbound and outbound connector design blueprints. Do not change an
exported name or signature without a matching blueprint change.

## Useful Commands

```sh
mise run deps     # Install dependencies for all languages
mise run lint     # Lint and type-check all languages
mise run format   # Format all languages
mise run test     # Run unit tests for all languages
mise run build    # Build all packages

# Run one language
mise run test:go
mise run lint:python
mise run build:typescript

# Run a single test
cd go && go test ./inbound/... -run TestNewServerNotImplemented
cd python && uv run pytest tests/test_stubs.py -k proxy
```

## Code Style & Conventions

- Fix lint errors before pushing. Run `mise run format` before fixing them by hand.
- Code comments state the mechanism and the reason. No incident history.

### Go

- `gci` enforces three import groups in order: standard library, third-party, local module
  (`github.com/temporalio/nexus-connector-mcp/go`). `mise run format` fixes the order.
- Tests use `testify` (`require`, not `assert`) and `t.Parallel()` where possible.

### Python

- The SDK wheel owns the `temporalio` package. Do not add `__init__.py` to `python/src/temporalio`.
- uv installs this package as a wheel, not in editable mode. The root `mise.toml` sets
  `UV_NO_EDITABLE=1`. Run Python commands through `mise run` or from a shell with mise activated.
- `temporalio.nexus_mcp` and `temporalio.nexus_mcp.workflow` must stay safe to import in Workflow
  code. Put handler-only imports in `temporalio.nexus_mcp.handler`.
- Type checkers: pyright, mypy, and basedpyright (`python/basedpyrightconfig.json`). Docstrings use
  the Google convention.

### TypeScript

- Prettier formats the code. ESLint enforces import order and type-only imports.
- Tests use ava and run from the compiled `lib/` output.

### License files

`python/LICENSE` and `typescript/LICENSE` are copies of the root `LICENSE`. The package builds
include them. Keep the three files identical.
