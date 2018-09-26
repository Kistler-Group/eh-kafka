default: services test

test:
	go test ./...
.PHONY: test

test_docker:
	docker-compose run --rm golang make test
.PHONY: test_docker

cover:
	go test -race -covermode=atomic -coverprofile=coverage.txt
.PHONY: cover

publish-coverage:
	curl -s https://codecov.io/bash > .codecov && \
	chmod +x .codecov && \
	./.codecov -f coverage.txt
.PHONY: publish-coverage

services:
	docker-compose pull zookeeper kafka
	docker-compose up -d zookeeper kafka
.PHONY: services

stop:
	docker-compose down
.PHONY: stop

clean:
	@find . -name \.covdecov -type f -delete
	@find . -name \.coverage.txt -type f -delete
	@rm -f gover.coverage
.PHONY: clean