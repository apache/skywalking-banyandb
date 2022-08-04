package cmd

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newUserGroupCmd() *cobra.Command {
	GroupCmd := &cobra.Command{
		Use:     "use",
		Version: version.Build(),
		Short:   "choose banyandb group",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/group/schema/" + args[0])
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: get " + addr + "/api/v1/group/schema/" + args[0])
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			if resp.StatusCode() == 404 { // check if the group exists
				return errors.New("no such group")
			} else {
				fmt.Println(resp.StatusCode())
				viper.Set("group", args[0])
				err = viper.WriteConfig()
				fmt.Println("switched to ", viper.Get("group"), "group")
			}
			return nil
		},
	}

	return GroupCmd
}
