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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
	"github.com/apache/skywalking-banyandb/pkg/auth"
)

const (
	timeRange      = 30 * time.Minute
	timeRangeUsage = `"start" and "end" specify a time range during which the query is preformed,
		they can be absolute time like "2006-01-02T15:04:05Z07:00"(https://www.rfc-editor.org/rfc/rfc3339), 
		or relative time (to the current time) like "-30m", "30m". 
		They are both optional and their default values follow the rules below: 
		1. when "start" and "end" are both absent, "start = now - 30 minutes" and "end = now", 
		namely past 30 minutes; 
		2. when "start" is absent and "end" is present, this command calculates "start" (minus 30 units), 
		e.g. "end = 2022-11-09T12:34:00Z", so "start = end - 30 minutes = 2022-11-09T12:04:00Z"; 
		3. when "start" is present and "end" is absent, this command calculates "end" (plus 30 units), 
		e.g. "start = 2022-11-09T12:04:00Z", so "end = start + 30 minutes = 2022-11-09T12:34:00Z".`
)

var errMalformedInput = errors.New("malformed input")

type reqBody struct {
	name       string
	group      string
	id         string
	ids        []string
	tags       []string
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

		var group string
		if metadata, ok := data["metadata"].(map[string]interface{}); ok {
			group, ok = metadata["group"].(string)
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
		} else if name, ok = data["name"].(string); ok {
			groups, ok := data["groups"].([]any)
			if ok {
				group = groups[0].(string)
			}
			if !ok && tryParseGroup {
				group = viper.GetString("group")
				if group == "" {
					return nil, errors.New("please specify a group through the input json or the config file")
				}
				data["groups"] = []string{group}
			}
		} else {
			return nil, errors.WithMessage(errMalformedInput, "absent node: name or groups")
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

func simpleParseFromYAML(reader io.Reader) (requests []reqBody, err error) {
	contents, err := file.Read(filePath, reader)
	if err != nil {
		return nil, err
	}
	for _, c := range contents {
		j, err := yaml.YAMLToJSON(c)
		if err != nil {
			return nil, err
		}
		requests = append(requests, reqBody{
			data: j,
		})
	}
	return requests, nil
}

func parseFromFlags() (requests []reqBody, err error) {
	if requests, err = parseGroupFromFlags(); err != nil {
		return nil, err
	}
	requests[0].name = name
	requests[0].id = id
	requests[0].ids = ids
	requests[0].tags = tags
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
	switch {
	case start == "" && end == "":
		startTS = time.Now().Add((-30) * time.Minute)
		endTS = time.Now()
	case start != "" && end != "":
		if startTS, err = parseTime(start); err != nil {
			return nil, err
		}
		if endTS, err = parseTime(end); err != nil {
			return nil, err
		}
	case start != "":
		if startTS, err = parseTime(start); err != nil {
			return nil, err
		}
		endTS = startTS.Add(timeRange)
	case end != "":
		if endTS, err = parseTime(end); err != nil {
			return nil, err
		}
		startTS = endTS.Add(-timeRange)
	}
	s := startTS.Format(time.RFC3339)
	e := endTS.Format(time.RFC3339)
	var rawRequests []reqBody
	if rawRequests, err = parseNameAndGroupFromYAML(reader); err != nil {
		return nil, err
	}
	for _, rb := range rawRequests {
		if rb.parsedData["timeRange"] != nil {
			requests = append(requests, rb)
			continue
		}
		timeRange := make(map[string]interface{})
		timeRange["begin"] = s
		timeRange["end"] = e
		rb.parsedData["timeRange"] = timeRange
		rb.data, err = json.Marshal(rb.parsedData)
		if err != nil {
			return nil, err
		}
		requests = append(requests, rb)
	}
	return requests, nil
}

func parseTime(timestamp string) (time.Time, error) {
	if len(timestamp) < 1 {
		return time.Time{}, errors.New("time is empty")
	}
	t, errAbsoluteTime := time.Parse(time.RFC3339, timestamp)
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
		id, ok = data["id"].(string)
		if !ok {
			return nil, errors.WithMessage(errMalformedInput, "absent node: id")
		}
		j, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
		requests = append(requests, reqBody{
			name:       name,
			group:      group,
			id:         id,
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

func rest(pfn paramsFn, fn reqFn, printer printer, enableTLS bool, insecure bool, cert string) (err error) {
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
		client := resty.New()
		if enableTLS {
			config := tls.Config{
				// #nosec G402
				InsecureSkipVerify: insecure,
			}
			if cert != "" {
				cert, err := os.ReadFile(cert)
				if err != nil {
					return err
				}
				certPool := x509.NewCertPool()
				if !certPool.AppendCertsFromPEM(cert) {
					return errors.New("failed to add server's certificate")
				}
				config.RootCAs = certPool
			}
			client.SetTLSClientConfig(&config)
		}
		req := client.R()
		// Add req headers.
		authHeader := getAuthHeader()
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
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
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("unexpected HTTP status code: %d", resp.StatusCode())
		}
		err = printer(i, r, bd)
		if err != nil {
			return err
		}
	}

	return nil
}

func getAuthHeader() string {
	if username != "" {
		return auth.GenerateBasicAuthHeader(username, password)
	}
	// If the command line username is not available, then look for it in the configuration file.
	user := viper.GetString("username")
	pwd := viper.GetString("password")
	if user == "" && pwd == "" {
		return ""
	}
	return auth.GenerateBasicAuthHeader(user, pwd)
}
