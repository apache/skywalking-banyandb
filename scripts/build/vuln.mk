GOVULNCHECK := $(tool_bin)/govulncheck
$(GOVULNCHECK):
	@echo "Install govulncheck..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
