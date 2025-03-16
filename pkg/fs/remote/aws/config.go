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

package aws

import "time"

type S3Config struct {
	Region         string
	KeyID          string
	SecretKey      string
	Endpoint       string
	Timeout        time.Duration
	MeasureBucket  string
	StreamBucket   string
	PropertyBucket string
}

var awsGlobalConfig *S3Config

func SetS3Config(cfg *S3Config) {
	awsGlobalConfig = cfg
}

func GetS3Config() *S3Config {
	return awsGlobalConfig
}
