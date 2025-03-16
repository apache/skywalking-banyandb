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
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

type s3FS struct {
	client     *s3.Client
	bucketName string
}

func NewFS(bucketName, region string) (remote.FS, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
	}

	client := s3.NewFromConfig(cfg)
	return &s3FS{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (s *s3FS) Upload(ctx context.Context, path string, data io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucketName,
		Key:    &path,
		Body:   data,
	})
	return err
}

func (s *s3FS) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucketName,
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
		Bucket: &s.bucketName,
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
		Bucket: &s.bucketName,
		Key:    &path,
	})
	return err
}

func (s *s3FS) Close() error {
	// No resources to close for S3 client
	return nil
}
