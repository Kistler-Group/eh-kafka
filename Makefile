default: services test

test:
	go test ./...
.PHONY: test

test_docker:
	docker-compose run --rm golang make test
.PHONY: test_docker

cover:
	go list -f '{{if len .TestGoFiles}}"go test -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}"{{end}}' ./... | xargs -L 1 sh -c
.PHONY: cover

services:
	docker-compose pull zookeeper kafka
	docker-compose up -d zookeeper kafka
.PHONY: services

stop:
	docker-compose down
.PHONY: stop

clean:
	@find . -name \.coverprofile -type f -delete
	@rm -f gover.coverprofile
.PHONY: clean
