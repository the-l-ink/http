# Contributing

This repository owns The Link's HTTP and WebSocket adapter.

## Development

Install the pinned toolchain and verify the package:

```sh
bun install --frozen-lockfile
bun run verify
```

Keep changes focused on this package's contract. Dependencies on another
The Link package must use its published release.

## Pull requests

Explain the contract served by the change, include focused proof for new
behavior, and keep each pull request focused on one coherent change.
