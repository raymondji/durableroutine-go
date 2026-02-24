# How-to guides

This project follows the "4 kinds of documentation" approach described here: https://diataxis.fr/

Each how-to guide contains a README.md that describes it and runnable code.

Each example can be run with `--backend=memory` (default, no Temporal required) or `--backend=temporal` (requires a running Temporal server).

```bash
go run docs/howto/reminder/cmd/main.go                    # uses in-memory backend
go run docs/howto/reminder/cmd/main.go --backend=temporal  # uses Temporal
```
