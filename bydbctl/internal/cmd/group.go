package cmd

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/ghodss/yaml"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
)

func newGroupCmd() *cobra.Command {
	GroupCmd := &cobra.Command{
		Use:     "group",
		Version: version.Build(),
		Short:   "banyandb group related Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
	}

	type MetaData struct { // "{\"group\":\"mxm\",\"name\":\"naonao\"}"
		Group string `json:"group"`
		Name  string `json:"name"`
	}

	GroupCreateCmd := &cobra.Command{
		Use:     "create",
		Version: version.Build(),
		Short:   "banyandb group schema Create Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			//logger.GetLogger().Info().Msg("banyandb group schema Create Operation")
			//client := resty.New()
			//addr, err := cmd.Flags().GetString("addr")
			//if err != nil {
			//	return err
			//}
			//metaData, err := cmd.Flags().GetString("json")
			//if err != nil {
			//	return err
			//}
			//var meta MetaData
			//err = json.Unmarshal([]byte(metaData), &meta)
			//if err != nil {
			//	return err
			//}
			//
			//fmt.Println("http://" + addr + "/api/v1/group/schema")
			//resp, err := client.R().Post("http://" + addr + "/api/v1/group/schema") //.SetBody()
			//if err != nil {
			//	return err
			//}
			//
			//yamlResult, err := yaml.JSONToYAML(resp.Body())
			//if err != nil {
			//	return err
			//}
			//fmt.Println(string(yamlResult))
			return nil
		},
	}

	GroupListCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "banyandb group schema List Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},

		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb group schema List Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/group/schema/lists")
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http://" + addr + "/api/v1/group/schema/lists")
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	// GroupGetCmd, GroupUpdateCmd, GroupDeleteCmd
	GroupCmd.AddCommand(GroupCreateCmd, GroupListCmd)
	return GroupCmd
}
