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
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

type s3FS struct {
	client *s3.Client
}

// todo: variable to get the bucket name for measure,property,stream
var cfg *AWSConfig

func NewFS() (remote.FS, error) {
	cfg = getAWSConfig()
	// timeout
	httpClient := &http.Client{
		Timeout: cfg.Timeout,
	}

	// todo: lots of configs to do
	awsCfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.KeyID,
			cfg.SecretKey,
			"",
		)),
		config.WithHTTPClient(httpClient),
		config.WithClientLogMode(aws.LogRetries),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config error: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &s3FS{
		client: client,
	}, nil
}

func (s *s3FS) Upload(ctx context.Context, path string, data io.Reader) error {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: getBucketName(),
		Key:    &path,
	})
	// todo: 细粒度的异常处理，比如网络超时重试
	if err != nil {
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
			err = nil
		} else {
			return fmt.Errorf("failed to check if object exists: %w", err)
		}
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: getBucketName(),
		Key:    &path,
		Body:   data,
	})
	return err
}

func (s *s3FS) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: getBucketName(),
		Key:    &path,
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *s3FS) List(ctx context.Context, prefix string) ([]string, error) {
	var files []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: getBucketName(),
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			files = append(files, *obj.Key)
		}
	}
	return files, nil
}

func (s *s3FS) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: getBucketName(),
		Key:    &path,
	})
	return err
}

func (s *s3FS) Close() error {
	// No resources to close for S3 client
	return nil
}

func getBucketName() *string {
	switch remote.NowCatalog {
	case "measure":
		return &awsGlobalConfig.MeasureBucket
	case "stream":
		return &awsGlobalConfig.StreamBucket
	case "property":
		return &awsGlobalConfig.PropertyBucket
	default:
		return nil
	}
}
