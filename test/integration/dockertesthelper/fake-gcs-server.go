package dockertesthelper

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

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "time"

    "cloud.google.com/go/storage"
    "github.com/ory/dockertest/v3"
    "github.com/ory/dockertest/v3/docker"
    "google.golang.org/api/option"
)

var (
    fakeGCSPool *dockertest.Pool
    fakeGCSRes  *dockertest.Resource
)

// InitFakeGCSServer starts a fake-gcs-server container for integration tests.
func InitFakeGCSServer() error {
    var err error
    fakeGCSPool, err = dockertest.NewPool("")
    if err != nil {
        return fmt.Errorf("docker unavailable: %w", err)
    }

    // Remove existing container if any
    containers, err := fakeGCSPool.Client.ListContainers(docker.ListContainersOptions{
        All: true,
        Filters: map[string][]string{
            "name": {FakeGCSContainerName},
        },
    })
    if err == nil {
        for _, c := range containers {
            _ = fakeGCSPool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: c.ID, Force: true})
        }
    }

    fakeGCSRes, err = fakeGCSPool.RunWithOptions(&dockertest.RunOptions{
        Repository: "fsouza/fake-gcs-server",
        Tag:        "latest",
        Cmd:        []string{"-scheme", "http", "-port", FakeGCSPort},
        Name:       FakeGCSContainerName,
    }, func(cfg *docker.HostConfig) {
        cfg.AutoRemove = true
        cfg.RestartPolicy = docker.RestartPolicy{Name: "no"}
        cfg.PortBindings = map[docker.Port][]docker.PortBinding{
            FakeGCSPort + "/tcp": {{HostIP: "0.0.0.0", HostPort: FakeGCSPort}},
        }
    })
    if err != nil {
        return fmt.Errorf("cannot start fake-gcs-server container: %w", err)
    }

    // Set env var so that storage client connects to emulator
    if err = os.Setenv("STORAGE_EMULATOR_HOST", "http://"+FakeGCSEndpoint); err != nil {
        return err
    }

    // Wait until server responds
    err = fakeGCSPool.Retry(func() error {
        resp, derr := http.Get("http://" + FakeGCSEndpoint + "/storage/v1/b")
        if derr != nil {
            return derr
        }
        _ = resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("fake gcs not ready: status %d", resp.StatusCode)
        }
        return nil
    })
    if err != nil {
        return err
    }

    // Create bucket for tests
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    client, err := storage.NewClient(ctx, option.WithoutAuthentication())
    if err != nil {
        return err
    }
    if err = client.Bucket(GCSBucketName).Create(ctx, "test-project", nil); err != nil {
        // bucket may already exist; ignore specific error
    }
    _ = client.Close()

    return nil
}

// CloseFakeGCSServer stops and removes the fake-gcs-server container.
func CloseFakeGCSServer() error {
    if fakeGCSPool == nil || fakeGCSRes == nil {
        return nil
    }
    return fakeGCSPool.Purge(fakeGCSRes)
}
