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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

// todo: Maybe we can bring in minio, oss
type s3FS struct {
	client   *s3.Client
	bucket   string
	basePath string
}

// NewFS creates a new instance of the file system for accessing S3 storage.
func NewFS(dest string) (remote.FS, error) {
	u, err := url.Parse(dest)
	if err != nil {
		return nil, fmt.Errorf("invalid dest URL: %w", err)
	}

	bucket, basePath := extractBucketAndBase(u)
	if bucket == "" {
		return nil, fmt.Errorf("bucket name not provided")
	}

	opts := []func(*config.LoadOptions) error{
		config.WithSharedConfigProfile("banyandb"),
		config.WithClientLogMode(aws.LogRetries),
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	return &s3FS{
		client:   client,
		bucket:   bucket,
		basePath: basePath,
	}, nil
}

func extractBucketAndBase(u *url.URL) (bucket, basePath string) {
	if u.Host != "" {
		return u.Host, strings.TrimPrefix(u.Path, "/")
	}
	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
	if len(parts) == 0 {
		return "", ""
	}
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func (s *s3FS) getFullPath(p string) string {
	if s.basePath == "" {
		return p
	}
	return path.Join(s.basePath, p)
}

func (s *s3FS) Upload(ctx context.Context, path string, data io.Reader) error {
	key := s.getFullPath(path)

	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(dataBytes)
	checksum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		Body:              bytes.NewReader(dataBytes),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(checksum),
	})
	if err != nil {
		var apiErr *smithy.OperationError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Error(), "BadDigest") {
			return fmt.Errorf("checksum validation failed: %w", err)
		}
		return err
	}
	return nil
}

func (s *s3FS) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	key := s.getFullPath(path)
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
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
