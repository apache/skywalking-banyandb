GINKGO := $(tool_bin)/ginkgo
$(GINKGO):
	@echo "Install ginkgo..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)
