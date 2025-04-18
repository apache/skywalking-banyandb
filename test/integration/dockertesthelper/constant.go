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

// Package dockertesthelper provides utilities and constants for managing Docker containers.
package dockertesthelper

// #nosec G101
const (
	// MinioContainerName is the name of the MinIO container.
	MinioContainerName = "banyandb-minio-test-abc"

	// BucketName is the name of the cloud storage bucket.
	BucketName = "test-bucket"

	// ConfigPath is the directory for MinIO configuration.
	ConfigPath = "/tmp/banyandb/s3/config"

	// CredentialsPath is the directory for MinIO credentials.
	CredentialsPath = "/tmp/banyandb/s3/credentials"

	// ConfigContent is the content of the MinIO config file.
	ConfigContent = `[default]
output = json
max_attempts = 2
region = us-east-1
endpoint_url = http://127.0.0.1:9481`

	// CredentialsContent is the content of the MinIO credentials file.
	CredentialsContent = `[default]
	aws_access_key_id = minioadmin
	aws_secret_access_key = minioadmin`

	// MinioEndpoint is the MinIO endpoint address.
	MinioEndpoint = "127.0.0.1:" + MinioPort

	// MinioPort is the port number for MinIO.
	MinioPort = "9481"

	// MinioRootUser is the root user for MinIO.
	MinioRootUser = "minioadmin"

	// MinioRootPassword is the root password for MinIO.
	MinioRootPassword = "minioadmin"
)
