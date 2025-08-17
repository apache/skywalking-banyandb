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

// Package config provides the configuration for the file system.
package config

// FsConfig represents the configuration for the file system.
type FsConfig struct {
	Azure    *AzureConfig `json:"azure,omitempty"`
	S3       *S3Config    `json:"s3,omitempty"`
	GCP      *GCPConfig   `json:"gcp,omitempty"`
	Provider string       `json:"provider"` // "azure", "s3" or "gcp"
}

// S3Config represents the configuration for S3.
type S3Config struct {
	// S3 configuration
	S3ConfigFilePath     string
	S3CredentialFilePath string
	S3ProfileName        string
	S3StorageClass       string
	S3ChecksumAlgorithm  string
}

// AzureConfig represents the configuration for Azure.
type AzureConfig struct {
	// Azure configuration
	// Azure Blob Storage configuration
	AzureAccountName string `json:"account_name"`
	AzureAccountKey  string `json:"account_key,omitempty"`
	AzureSASToken    string `json:"sas_token,omitempty"`
	Container        string `json:"container"`
	AzureEndpoint    string `json:"endpoint"`
}

// GCPConfig represents the configuration for GCP (Google Cloud Storage).
type GCPConfig struct {
	// Path to a GCP service account JSON file. If empty, the default credentials will be used.
	GCPServiceAccountFile string `json:"service_account_file,omitempty"`
	// Optional bucket name override. If empty, the bucket name is inferred from the path provided to NewFS.
	Bucket string `json:"bucket,omitempty"`
}
