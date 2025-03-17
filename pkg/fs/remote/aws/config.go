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

// S3Config holds the configuration for S3 storage.
type S3Config struct {
	Region         string
	KeyID          string
	SecretKey      string
	Endpoint       string
	MeasureBucket  string
	StreamBucket   string
	PropertyBucket string
	Timeout        time.Duration
}

var awsGlobalConfig *S3Config

// SetS3Config configures the S3 settings based on the provided config.
func SetS3Config(cfg *S3Config) {
	awsGlobalConfig = cfg
}

// GetS3Config retrieves the current S3 configuration.
func GetS3Config() *S3Config {
	return awsGlobalConfig
}
