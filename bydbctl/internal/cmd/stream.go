package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/ghodss/yaml"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newStreamCmd() *cobra.Command {
	StreamCmd := &cobra.Command{
		Use:     "stream",
		Version: version.Build(),
		Short:   "banyandb stream schema related Operation",
		//PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
		//	if err = config.Load("logging", cmd.Flags()); err != nil {
		//		return err
		//	}
		//	return logger.Init(logging)
		//},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
	}

	type Metadata struct { // "{\"group\":\"group1\",\"name\":\"name1\"}"
		Group string `json:"group"`
		Name  string `json:"name"`
	}

	type Stream struct {
		Metadata Metadata `json:"metadata"`
		// todo: add support for other properties except for metadata
	}

	type StreamReq struct {
		Stream Stream `json:"stream"`
	}

	StreamCreateCmd := &cobra.Command{
		Use:     "create",
		Version: version.Build(),
		Short:   "banyandb stream schema create Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Create Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			streamReq := StreamReq{}
			err = json.Unmarshal([]byte(body), &streamReq)
			if err != nil {
				return err
			}
			if streamReq.Stream.Metadata.Group == "" {
				streamReq.Stream.Metadata.Group = fmt.Sprintf("%v", viper.Get("group"))
				if streamReq.Stream.Metadata.Group == "" {
					return errors.New("should choose a group")
				}
			}
			resp, err := client.R().SetBody(streamReq).Post("http://" + addr + "/api/v1/stream/schema")
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: post " + addr + "/api/v1/stream/schema")
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamUpdateCmd := &cobra.Command{
		Use:     "update",
		Version: version.Build(),
		Short:   "banyandb stream schema Update Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Update Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			if err != nil {
				return err
			}
			streamReq := StreamReq{}
			err = json.Unmarshal([]byte(body), &streamReq)
			if err != nil {
				return err
			}
			if streamReq.Stream.Metadata.Group == "" {
				streamReq.Stream.Metadata.Group = fmt.Sprintf("%v", viper.Get("group"))
				if streamReq.Stream.Metadata.Group == "" {
					return errors.New("should choose a group")
				}
			}
			resp, err := client.R().SetBody(streamReq).Put("http://" + addr + "/api/v1/stream/schema")
			if err != nil {
				return err
			}
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamGetCmd := &cobra.Command{
		Use:     "get",
		Version: version.Build(),
		Short:   "banyandb stream schema Get Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Get Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			stream := Stream{} // metadata
			err = json.Unmarshal([]byte(body), &stream)
			if err != nil {
				return err
			}
			if stream.Metadata.Group == "" {
				stream.Metadata.Group = fmt.Sprintf("%v", viper.Get("group"))
				if stream.Metadata.Group == "" {
					return errors.New("should choose a group")
				}
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/stream/schema/" + stream.Metadata.Group + "/" + stream.Metadata.Name)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: get " + addr + "/api/v1/stream/schema/" + stream.Metadata.Group + "/" + stream.Metadata.Name)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamDeleteCmd := &cobra.Command{
		Use:     "delete",
		Version: version.Build(),
		Short:   "banyandb stream schema Delete Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Delete Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			stream := Stream{} // metadata
			err = json.Unmarshal([]byte(body), &stream)
			if err != nil {
				return err
			}
			if stream.Metadata.Group == "" {
				stream.Metadata.Group = fmt.Sprintf("%v", viper.Get("group"))
				if stream.Metadata.Group == "" {
					return errors.New("should choose a group")
				}
			}
			resp, err := client.R().Delete("http://" + addr + "/api/v1/stream/schema/" + stream.Metadata.Group + "/" + stream.Metadata.Name)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: delete " + addr + "/api/v1/stream/schema/" + stream.Metadata.Group + "/" + stream.Metadata.Name)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamListCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "banyandb stream schema List Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema List Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			if err != nil {
				return err
			}
			var meta Metadata
			err = json.Unmarshal([]byte(body), &meta)
			if err != nil {
				return err
			}
			if meta.Group == "" {
				meta.Group = fmt.Sprintf("%v", viper.Get("group"))
				if meta.Group == "" {
					return errors.New("should choose a group")
				}
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/stream/schema/lists/" + meta.Group)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: list " + "http://" + addr + "/api/v1/stream/schema/" + meta.Group)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	// todo: replace the complex struct with map?
	//type Timestamp struct {
	//	seconds int64
	//	nanos   int32
	//}
	//
	//type TimeRange struct {
	//	begin Timestamp
	//	end   Timestamp
	//}
	//
	//type QueryRequest struct {
	//	metadata  Metadata
	//	timeRange TimeRange
	//  ...
	//}

	StreamQueryCmd := &cobra.Command{
		Use:     "query",
		Version: version.Build(),
		Short:   "banyandb stream schema Query Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			//logger.GetLogger().Info().Msg("banyandb stream schema Query Operation")
			//client := resty.New()
			//addr, err := cmd.Flags().GetString("addr")
			//if err != nil {
			//	return err
			//}
			//body, err := cmd.Flags().GetString("json")
			//queryReq := QueryRequest{}
			//err = json.Unmarshal([]byte(body), &queryReq)
			//if err != nil {
			//	return err
			//}
			//if queryReq.metadata.Group == "" {
			//	queryReq.metadata.Group = fmt.Sprintf("%v", viper.Get("group"))
			//	if queryReq.metadata.Group == "" {
			//		return errors.New("error: should choose a group")
			//	}
			//}
			//resp, err := client.R().SetBody(queryReq).Post("http://" + addr + "/api/v1/stream/data")
			//if err != nil {
			//	return err
			//}
			//logger.GetLogger().Info().Msg("http request: post " + addr + "/api/v1/stream/data")
			//logger.GetLogger().Info().Msg("http response: " + resp.Status())
			//yamlResult, err := yaml.JSONToYAML(resp.Body())
			//if err != nil {
			//	return err
			//}
			//fmt.Println(string(yamlResult))
			return nil
		},
	}
	StreamCmd.AddCommand(StreamGetCmd, StreamCreateCmd, StreamDeleteCmd, StreamUpdateCmd, StreamListCmd, StreamQueryCmd)
	return StreamCmd
}
