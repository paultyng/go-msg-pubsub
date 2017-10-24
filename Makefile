all: ci

metalinter:
	gometalinter --config .gometalinter.json
.PHONY: metalinter

test:
	go test -v -cover .
.PHONY: test

ci: metalinter test
.PHONY: ci