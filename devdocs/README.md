# Developer documentation

Developer-facing documentation

## Local Temporal setup

Integration tests require a Temporal server running at `localhost:7233`.

### Automatic setup (Claude Code web)

The `.claude/setup.sh` script installs the Temporal CLI automatically when
configured as the **Setup script** in your Claude Code web environment settings.
It runs once per new session, before Claude launches.

### Manual setup

Install the Temporal CLI:

```bash
go install github.com/temporalio/cli/cmd/temporal@latest
```

### Running the server

Start a local Temporal dev server (in-memory, no persistence):

```bash
make temporal-start
```

This starts Temporal on port `7233` with the web UI on port `8233`.

### Running tests

With the Temporal server running in another terminal:

```bash
make test               # all tests
make test-integration   # only Temporal integration tests
```
