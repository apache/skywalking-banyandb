package cmd

import (
	"github.com/spf13/cobra"
)

func newBanyanDBCmd() []*cobra.Command {
	UseGroupCmd := newUserGroupCmd()
	GroupCmd := newGroupCmd()
	StreamCmd := newStreamCmd()
	// todo: add more commands
	//MeasureCmd := newMeasureCmd()
	//PropertyCmd := newPropertyCmd()
	//IndexRuleCmd := newIndexRuleCmd()
	//IndexRuleBindingCmd := newIndexRuleBindingCmd()

	return []*cobra.Command{UseGroupCmd, GroupCmd, StreamCmd}
}
