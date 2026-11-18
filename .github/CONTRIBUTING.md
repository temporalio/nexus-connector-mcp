# Contributing

## Dependencies

This project uses [mise] to pin every tool it depends on: Go, golangci-lint, Python, uv, Node, and pnpm.
Install mise, then run `mise install` in the repository root. Then run `mise run deps` to install
the dependencies for each language.

## Useful Commands

`mise` is also the task runner. Run `mise tasks` to list all tasks.

| Command           | Description                          |
| ----------------- | ------------------------------------ |
| `mise run deps`   | Install dependencies                 |
| `mise run lint`   | Lint and type-check                  |
| `mise run format` | Format                               |
| `mise run test`   | Run unit tests                       |
| `mise run build`  | Build packages                       |

Each task has one subtask per language, for example `mise run lint:go`, `mise run test:python`, and
`mise run build:typescript`. CI runs the `deps`, `lint`, `test`, and `build` subtasks for each language.

[mise]: https://mise.jdx.dev/
