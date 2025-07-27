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

	// Azurite constants
	// AzuriteContainerName is the name of the Azurite container.
	AzuriteContainerName = "banyandb-azurite-test"

	// AzuritePort is the blob service port of Azurite.
	AzuritePort = "10000"

	// AzuriteEndpoint is the host endpoint of Azurite blob service.
	AzuriteEndpoint = "127.0.0.1:" + AzuritePort

	// AzuriteConnStr is the Azure SDK connection string pointing to the local Azurite instance.
	// This is the well-known connection string for Azurite with default credentials.
	AzuriteConnStr = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
		"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
		"BlobEndpoint=http://127.0.0.1:" + AzuritePort + "/devstoreaccount1;"

	// AzuriteContainer is the default container (bucket) used in tests.
	AzuriteContainer = "test-container"

	// Fake GCS server constants
	// FakeGCSContainerName is the name of the fake-gcs-server container.
	FakeGCSContainerName = "banyandb-fake-gcs-test"
	// FakeGCSPort is the HTTP port exposed by fake-gcs-server (using --scheme http).
	FakeGCSPort = "4444"
	// FakeGCSEndpoint is the host endpoint for fake-gcs-server.
	FakeGCSEndpoint = "127.0.0.1:" + FakeGCSPort
    // GCSBucketName is the bucket used in integration tests.
    GCSBucketName = "test-bucket"
)
