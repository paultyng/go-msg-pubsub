NOVENDOR = $(shell go list ./... | grep -v vendor)
NOVENDOR_LINTER = $(shell go list ./... | grep -v vendor | sed "s|github.com/paultyng/go-msg-pubsub|.|")

metalinter:
	gometalinter --config .gometalinter.json $(NOVENDOR_LINTER)
.PHONY: metalinter

test:
	go test -v -cover $(NOVENDOR)
.PHONY: test

ci: metalinter test
.PHONY: ci