.PHONY: test vet fmt check

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

check: fmt test vet
