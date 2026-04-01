# Contributing

Thank you for your interest in contributing.

## Development Setup

1. Fork and clone the repository
2. Copy `.env.example` to `.env` and configure
3. Install dependencies
4. Create a feature branch from `develop` (or `main` if no `develop` branch exists)

## Branch Strategy

| Branch | Purpose |
|---|---|
| `main` | Production — tagged releases only |
| `develop` | Integration branch — PRs target here |
| `feature/*` | New features |
| `fix/*` | Bug fixes |
| `hotfix/*` | Urgent production fixes |

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new feature
fix: resolve bug
docs: update documentation
test: add or update tests
chore: maintenance tasks
refactor: code restructure without behavior change
ci: CI/CD changes
security: security improvements
```

## Pull Requests

- Fill out the PR template completely
- Ensure all tests pass
- Keep PRs focused — one feature or fix per PR
- Reference related issues

## AI-Assisted Development

This project uses AI-assisted development tools (Cursor, Claude Code, GitHub Copilot).
All code is reviewed by the maintainer before merge. See `AGENTS.md` for AI agent rules.

## Security

Report vulnerabilities via the process described in `SECURITY.md`.
Do not open public issues for security concerns.
