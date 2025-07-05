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

// Package aws provides an AWS S3 file system implementation.
package aws

import (
	"context"
	"fmt"
	config2 "github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

// todo: Maybe we can bring in minio, oss
type s3FS struct {
	client            *s3.Client
	bucket            string
	basePath          string
	checksumAlgorithm types.ChecksumAlgorithm
	storageClass      types.StorageClass
}

// NewFS creates a new instance of the file system for accessing S3 storage.
func NewFS(path string, userConfig *config2.FsConfig) (remote.FS, error) {
	bucket, basePath := extractBucketAndBase(path)
	if bucket == "" {
		return nil, fmt.Errorf("bucket name not provided")
	}
	if userConfig == nil {
		return nil, fmt.Errorf("userConfig is nil")
	}
	if userConfig.S3 == nil {
		return nil, fmt.Errorf("S3 config is nil")
	}

	opts := buildAWSCfgOptions(userConfig)

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	fs := &s3FS{
		client:   client,
		bucket:   bucket,
		basePath: basePath,
	}

	if userConfig.S3.S3ChecksumAlgorithm != "" {
		fs.checksumAlgorithm = types.ChecksumAlgorithm(userConfig.S3.S3ChecksumAlgorithm)
	}
	if userConfig.S3.S3StorageClass != "" {
		fs.storageClass = types.StorageClass(userConfig.S3.S3StorageClass)
	}
	return fs, nil
}

func buildAWSCfgOptions(userConfig *config2.FsConfig) []func(*config.LoadOptions) error {
	s3Config := userConfig.S3
	opts := []func(*config.LoadOptions) error{
		config.WithClientLogMode(aws.LogRetries),
	}

	if s3Config.S3ProfileName != "" {
		opts = append(opts, config.WithSharedConfigProfile(s3Config.S3ProfileName))
	}
	if s3Config.S3ConfigFilePath != "" {
		opts = append(opts, config.WithSharedConfigFiles([]string{s3Config.S3ConfigFilePath}))
	}
	if s3Config.S3CredentialFilePath != "" {
		opts = append(opts, config.WithSharedCredentialsFiles([]string{s3Config.S3CredentialFilePath}))
	}

	return opts
}

func extractBucketAndBase(path string) (bucket, basePath string) {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return "", ""
	}

	parts := strings.SplitN(trimmedPath, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		basePath = parts[1]
	}
	return
}

func (s *s3FS) getFullPath(p string) string {
	if s.basePath == "" {
		return p
	}
	return path.Join(s.basePath, p)
}

func (s *s3FS) Upload(ctx context.Context, path string, data io.Reader) error {
	key := s.getFullPath(path)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		Body:              data,
		ChecksumAlgorithm: s.checksumAlgorithm,
		StorageClass:      s.storageClass,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *s3FS) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	key := s.getFullPath(path)
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *s3FS) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.getFullPath(prefix)
	var files []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			if s.basePath != "" {
				key = strings.TrimPrefix(key, s.basePath+"/")
			}
			files = append(files, key)
		}
	}
	return files, nil
}

func (s *s3FS) Delete(ctx context.Context, path string) error {
	key := s.getFullPath(path)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *s3FS) Close() error {
	// No resources to close for S3 client
	return nil
}
