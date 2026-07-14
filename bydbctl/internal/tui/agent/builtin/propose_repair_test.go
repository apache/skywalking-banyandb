package builtin

import (
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
)

func TestProposeToolFailedDetectsStructuredFailure(t *testing.T) {
	if !proposeToolFailed(bridge.ToolProposeQueryPlan, `{"valid":false,"message":"unknown column"}`) {
		t.Fatal("expected structured propose failure to be detected")
	}
	if proposeToolFailed(bridge.ToolProposeQueryPlan, `{"valid":true,"query":"SELECT 1"}`) {
		t.Fatal("expected successful propose result to be ignored")
	}
}
