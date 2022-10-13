// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	str2duration "github.com/xhit/go-str2duration/v2"
	"go.uber.org/multierr"
	stpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/bydbctl/pkg/file"
)

const (
	// RFC3339 refers to https://www.rfc-editor.org/rfc/rfc3339
	RFC3339   = "2006-01-02T15:04:05Z07:00"
	timeRange = 30 * time.Minute
)

var errMalformedInput = errors.New("malformed input")

type reqBody struct {
	name       string
	group      string
	id         string
	tag        string
	parsedData map[string]interface{}
	data       []byte
}

type request struct {
	req *resty.Request
	reqBody
}

type reqFn func(request request) (*resty.Response, error)

type paramsFn func() ([]reqBody, error)

func parseNameAndGroupFromYAML(reader io.Reader) (requests []reqBody, err error) {
	return parseFromYAML(true, reader)
}

func parseNameFromYAML(reader io.Reader) (requests []reqBody, err error) {
	return parseFromYAML(false, reader)
}

func parseFromYAML(tryParseGroup bool, reader io.Reader) (requests []reqBody, err error) {
	contents, err := file.Read(filePath, reader)
	if err != nil {
		return nil, err
	}
	for _, c := range contents {
		j, err := yaml.YAMLToJSON(c)
		if err != nil {
			return nil, err
		}
		var data map[string]interface{}
		err = json.Unmarshal(j, &data)
		if err != nil {
			return nil, err
		}
		metadata, ok := data["metadata"].(map[string]interface{})
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: metadata")
		}
		group, ok := metadata["group"].(string)
		if !ok && tryParseGroup {
			group = viper.GetString("group")
			if group == "" {
				return nil, errors.New("please specify a group through the input json or the config file")
			}
			metadata["group"] = group
		}
		name, ok = metadata["name"].(string)
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: name in metadata")
		}
		j, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
		requests = append(requests, reqBody{
			name:       name,
			group:      group,
			data:       j,
			parsedData: data,
		})
	}
	return requests, nil
}

func parseFromFlags() (requests []reqBody, err error) {
	if requests, err = parseGroupFromFlags(); err != nil {
		return nil, err
	}
	requests[0].name = name
	return requests, nil
}

func parseGroupFromFlags() ([]reqBody, error) {
	group := viper.GetString("group")
	if group == "" {
		return nil, errors.New("please specify a group through the flag or the config file")
	}
	return []reqBody{{group: group}}, nil
}

func parseTimeRangeFromFlagAndYAML(reader io.Reader) (requests []reqBody, err error) {
	var startTS, endTS time.Time
	if start == "" && end == "" {
		startTS = time.Now().Add((-30) * time.Minute)
		endTS = time.Now()
	} else if start != "" && end != "" {
		if startTS, err = parseTime(start); err != nil {
			return nil, err
		}
		if endTS, err = parseTime(start); err != nil {
			return nil, err
		}
	} else if start != "" {
		if startTS, err = parseTime(start); err != nil {
			return nil, err
		}
		endTS = startTS.Add(timeRange)
	} else {
		if endTS, err = parseTime(end); err != nil {
			return nil, err
		}
		startTS = endTS.Add(-timeRange)
	}
	s := startTS.Format(RFC3339)
	e := endTS.Format(RFC3339)
	if requests, err = parseNameAndGroupFromYAML(reader); err != nil {
		return nil, err
	}
	for _, rb := range requests {
		if rb.parsedData["timeRange"] != nil {
			continue
		}
		timeRange := make(map[string]interface{})
		timeRange["start"] = s
		timeRange["end"] = e
		rb.parsedData["timeRange"] = timeRange
		rb.data, err = json.Marshal(rb.parsedData)
		if err != nil {
			return nil, err
		}
	}
	return requests, nil
}

func parseTime(timestamp string) (time.Time, error) {
	if len(timestamp) < 1 {
		return time.Time{}, errors.New("time is empty")
	}
	t, errAbsoluteTime := time.Parse(timestamp, RFC3339)
	if errAbsoluteTime == nil {
		return t, nil
	}
	duration, err := str2duration.ParseDuration(timestamp)
	if err != nil {
		return time.Time{}, errors.WithMessagef(multierr.Combine(errAbsoluteTime, err), "time %s is neither absolute time nor relative time", timestamp)
	}
	return time.Now().Add(duration), nil
}

func parseFromYAMLForProperty(reader io.Reader) (requests []reqBody, err error) {
	contents, err := file.Read(filePath, reader)
	if err != nil {
		return nil, err
	}
	for _, c := range contents {
		j, err := yaml.YAMLToJSON(c)
		if err != nil {
			return nil, err
		}
		var data map[string]interface{}
		err = json.Unmarshal(j, &data)
		if err != nil {
			return nil, err
		}
		metadata, ok := data["metadata"].(map[string]interface{})
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: metadata")
		}
		container, ok := metadata["container"].(map[string]interface{})
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: container")
		}
		group, ok := container["group"].(string)
		if !ok {
			group = viper.GetString("group")
			if group == "" {
				return nil, errors.New("please specify a group through the input json or the config file")
			}
			metadata["group"] = group
		}
		name, ok = container["name"].(string)
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: name in metadata")
		}
		id, ok := metadata["id"].(string)
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: id")
		}
		//tags, ok := data["tag"].(map[string]interface{})
		tags, ok := data["tag"].([]string)
		j, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
		requests = append(requests, reqBody{
			name:       name,
			group:      group,
			id:         id,
			tag:        strings.Join(tags, ","),
			data:       j,
			parsedData: data,
		})
	}
	return requests, nil
}

type printer func(index int, reqBody reqBody, body []byte) error

func yamlPrinter(index int, _ reqBody, body []byte) error {
	yamlResult, err := yaml.JSONToYAML(body)
	if err != nil {
		return err
	}
	if index > 0 {
		fmt.Println("---")
	}
	fmt.Print(string(yamlResult))
	fmt.Println()
	return nil
}

func rest(pfn paramsFn, fn reqFn, printer printer) (err error) {
	var requests []reqBody
	if pfn == nil {
		requests = []reqBody{{}}
	} else {
		requests, err = pfn()
		if err != nil {
			return err
		}
	}

	for i, r := range requests {
		req := resty.New().R()
		resp, err := fn(request{
			reqBody: r,
			req:     req,
		})
		if err != nil {
			return err
		}
		bd := resp.Body()
		var st *stpb.Status
		err = json.Unmarshal(bd, &st)
		if err == nil && st.Code != int32(codes.OK) {
			s := status.FromProto(st)
			return s.Err()
		}
		err = printer(i, r, bd)
		if err != nil {
			return err
		}
	}

	return nil
}
