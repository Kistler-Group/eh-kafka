[![Build Status](https://travis-ci.com/Kistler-Group/eh-kafka.svg?branch=master)](https://travis-ci.com/Kistler-Group/eh-kafka)
[![Coverage Status](https://codecov.io/gh/Kistler-Group/eh-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/Kistler-Group/eh-kafka)
[![GoDoc](https://godoc.org/github.com/Kistler-Group/eh-kafka?status.svg)](https://godoc.org/github.com/Kistler-Group/eh-kafka)
[![Go Report Card](https://goreportcard.com/badge/Kistler-Group/eh-kafka)](https://goreportcard.com/report/Kistler-Group/eh-kafka)

# Event Horizon Kafka

Event Horizon Kafka is event bus for [Event Horizon] a CQRS/ES toolkit for Go.

[Event Horizon]: https://github.com/looplab/eventhorizon

# Usage

See the eventbus_test.go how to initialize.

## Development

To develop Event Horizon Kafka you need to have Docker and Docker Compose installed.

To start all needed services and run all tests, simply run make:

```bash
make
```

To manualy run the services and stop them:

```bash
make services
make stop
```

When the services are running testing can be done either locally or with Docker:

```bash
make test
make test_docker
go test ./...
```

The difference between `make test` and `go test ./...` is that `make test` also prints coverage info.

# License

GNU LESSER GENERAL PUBLIC LICENSE Version 2.1, February 1999