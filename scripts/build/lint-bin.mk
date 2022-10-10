
LINTER := $(tool_bin)/golangci-lint
$(LINTER):
	@GOBIN=$(tool_bin) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.50.0
