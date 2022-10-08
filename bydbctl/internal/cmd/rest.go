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
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	str2duration "github.com/xhit/go-str2duration/v2"
	stpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/bydbctl/pkg/file"
)

const (
	RFC3339 = "2006-01-02T15:04:05Z07:00"
)

var errMalformedInput = errors.New("malformed input")

type reqBody struct {
	name  string
	group string
	data  []byte
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
			name:  name,
			group: group,
			data:  j,
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
		if !ok {
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
		timeRange, ok := data["timeRange"].(map[string]interface{})
		if !ok { // parse from flag if timeRange is absent in command/file
			if start == "" && end == "" { // both are absent
				start = time.Now().Add((-30) * time.Minute).Format(RFC3339)
				end = time.Now().Format(RFC3339)
			} else if start == "" && end != "" { // only end exists
				ok, durationUnit, err := isDuration(end)
				if err != nil {
					return nil, err
				}
				if ok { // end is relative time
					duration, err := str2duration.ParseDuration(end)
					if err != nil {
						return nil, err
					}
					end = time.Now().Add(-1 * duration).Format(RFC3339)
					start = time.Now().Add(-1 * duration).Add(time.Duration(-30 * durationUnit)).Format(RFC3339)
				} else { // end is absolute time
					endTimeStamp, err := time.Parse(RFC3339, end)
					if err != nil {
						return nil, err
					}
					start = endTimeStamp.Add(time.Duration(-30) * time.Minute).Format(RFC3339)
				}
			} else if start != "" && end == "" { // only start exists
				ok, durationUnit, err := isDuration(end)
				if err != nil {
					return nil, err
				}
				if ok { // start is relative time
					duration, err := str2duration.ParseDuration(start)
					if err != nil {
						return nil, err
					}
					start = time.Now().Add(-1 * duration).Format(RFC3339)
					end = time.Now().Add(-1 * duration).Add(time.Duration(30 * durationUnit)).Format(RFC3339)
				} else { // start is absolute time
					startTimeStamp, err := time.Parse(RFC3339, start)
					if err != nil {
						return nil, err
					}
					end = startTimeStamp.Add(time.Duration(30) * time.Minute).Format(RFC3339)
				}
			} else { // both exist
				ok, _, err = isDuration(start)
				if err != nil {
					return nil, err
				}
				if ok { // start is relative time
					duration, err := str2duration.ParseDuration(start)
					if err != nil {
						return nil, err
					}
					start = time.Now().Add(-1 * duration).Format(RFC3339)
				}
				ok, _, err = isDuration(end)
				if err != nil {
					return nil, err
				}
				if ok { // end is relative time
					duration, err := str2duration.ParseDuration(end)
					if err != nil {
						return nil, err
					}
					end = time.Now().Add(-1 * duration).Format(RFC3339)
				}
			}
			timeRange = make(map[string]interface{})
			timeRange["start"] = start
			timeRange["end"] = end
		}
		j, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
		requests = append(requests, reqBody{
			name:  name,
			group: group,
			data:  j,
		})
	}
	return requests, nil
}

func isDuration(timeStamp string) (bool, int64, error) { // if timeStamp is duration(relative time), return (true, the uint of timestamp, nil).
	if len(timeStamp) < 2 {
		return false, -1, errors.New("the given time is neither absolute time nor relative time")
	}
	if timeStamp[len(timeStamp)-2] == 'u' {
		return false, -1, errors.New("the time uint us is unsupported")
	} else if timeStamp[len(timeStamp)-2] == 'n' {
		return false, -1, errors.New("the time uint ns is unsupported")
	}
	switch timeStamp[len(timeStamp)-1] {
	case 'w':
		return true, int64(time.Hour) * 168, nil
	case 'd':
		return true, int64(time.Hour) * 24, nil
	case 'h':
		return true, int64(time.Hour), nil
	case 'm':
		return true, int64(time.Minute), nil
	case 's':
		if timeStamp[len(timeStamp)-2] == 'm' {
			return true, int64(time.Millisecond), nil
		}
		return true, int64(time.Second), nil
	}
	return false, -1, nil
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
