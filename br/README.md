# TiKV-BR

[![Build Status](https://github.com/tikv/migration/actions/workflows/ci-br.yml/badge.svg)](https://github.com/tikv/migration/actions/workflows/ci-br.yml)
[![codecov](https://codecov.io/gh/tikv/migration/branch/main/graph/badge.svg?token=7nmbrqKeWs&flag=br)](https://app.codecov.io/gh/tikv/migration/tree/main/br)
[![LICENSE](https://img.shields.io/github/license/tikv/migration)](https://github.com/tikv/migration/blob/main/br/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tikv/migration/br)](https://goreportcard.com/report/github.com/tikv/migration/br)

**TiKV Backup & Restore (TiKV-BR)** is a command-line tool for distributed backup and restoration of the TiKV cluster data.

## Architecture

<img src="docs/images/tikv-br-architecture.png?sanitize=true" alt="architecture" width="600"/>

## Building

To build binary and run test:

```bash
$ make build   // build the binary with debug info
$ make release // build the release binary used for production
$ make test    // run unit test
```

*Notice TiKV-BR requires building with Go version `Go >= 1.21`*

When TiKV-BR is built successfully, you can find binary in the `bin` directory.

## User Manual

For details, see [TiKV-BR User Docs](https://tikv.org/docs/latest/concepts/explore-tikv-features/backup-restore/).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiKV-BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
