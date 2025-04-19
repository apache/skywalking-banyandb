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

package dockertesthelper

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	casesbackup "github.com/apache/skywalking-banyandb/test/cases/backup"
)

var (
	minioPool *dockertest.Pool
	minioRes  *dockertest.Resource
	// BucketName is the name of the bucket used in integration tests.
	BucketName = "test-bucket"
	// S3ConfigPath is the path of s3 config file, it is random.
	S3ConfigPath string
	// S3CredentialsPath is the path of s3 credentials file, it is random.
	S3CredentialsPath string
)

// InitMinIOContainer initializes a MinIO container for integration tests.
func InitMinIOContainer() error {
	var err error
	tempDir, err := os.MkdirTemp("", "banyandb-s3-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	S3ConfigPath = filepath.Join(tempDir, "config")
	S3CredentialsPath = filepath.Join(tempDir, "credentials")
	casesbackup.SharedContext.BucketName = BucketName
	if err = os.WriteFile(S3ConfigPath, []byte(ConfigContent), 0o600); err != nil {
		return fmt.Errorf("cannot write config file: %w", err)
	}
	if err = os.WriteFile(S3CredentialsPath, []byte(CredentialsContent), 0o600); err != nil {
		return fmt.Errorf("cannot write credentials file: %w", err)
	}

	minioPool, err = dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("cannot connect to Docker: %w", err)
	}

	minioContainerName := MinioContainerName
	containers, err := minioPool.Client.ListContainers(docker.ListContainersOptions{
		All: true,
		Filters: map[string][]string{
			"name": {minioContainerName},
		},
	})
	if err != nil {
		log.Fatalf("cannot list container: %s", err)
	}
	for _, container := range containers {
		err = minioPool.Client.RemoveContainer(docker.RemoveContainerOptions{
			ID:    container.ID,
			Force: true,
		})
		if err != nil {
			log.Fatalf("connot remove old container %s: %s", container.ID, err)
		}
	}

	minioRes, err = minioPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "quay.io/minio/minio",
		Tag:        "RELEASE.2025-04-08T15-41-24Z",
		Cmd:        []string{"server", "/data"},
		Name:       minioContainerName,
		Env: []string{
			"MINIO_ROOT_USER=" + MinioRootUser,
			"MINIO_ROOT_PASSWORD=" + MinioRootPassword,
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.PortBindings = map[docker.Port][]docker.PortBinding{
			"9000/tcp": {{HostIP: "0.0.0.0", HostPort: "9481"}},
		}
	})
	if err != nil {
		return fmt.Errorf("cannot start Minio container: %w", err)
	}

	endpoint := MinioEndpoint

	err = minioPool.Retry(func() error {
		minioClient, initErr := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(MinioRootUser, MinioRootPassword, ""),
			Secure: false,
		})
		if initErr != nil {
			return initErr
		}
		return minioClient.MakeBucket(context.Background(), BucketName, minio.MakeBucketOptions{})
	})
	if err != nil {
		return err
	}
	return nil
}

// CloseMinioContainer stops and removes the MinIO container used in integration tests.
func CloseMinioContainer() error {
	return minioPool.Purge(minioRes)
}
