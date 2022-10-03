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

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	stpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/bydbctl/pkg/file"
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
