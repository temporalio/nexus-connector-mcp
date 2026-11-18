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

## Code Style & Conventions

- Code comments state the mechanism and the reason. No incident history.
