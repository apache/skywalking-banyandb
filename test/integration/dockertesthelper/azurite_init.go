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
	"fmt"
	"net"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var (
	azuritePool *dockertest.Pool
	azuriteRes  *dockertest.Resource
)

// InitAzuriteContainer starts a fresh Azurite container for integration tests.
func InitAzuriteContainer() error {
	var err error
	azuritePool, err = dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("cannot connect to Docker: %w", err)
	}

	// Ensure any existing container is removed first.
	containers, err := azuritePool.Client.ListContainers(docker.ListContainersOptions{
		All: true,
		Filters: map[string][]string{
			"name": {AzuriteContainerName},
		},
	})
	if err != nil {
		return fmt.Errorf("cannot list containers: %w", err)
	}

	for _, container := range containers {
		if err := azuritePool.Client.RemoveContainer(docker.RemoveContainerOptions{
			ID:    container.ID,
			Force: true,
		}); err != nil {
			return fmt.Errorf("cannot remove old container %s: %w", container.ID, err)
		}
	}

	azuriteRes, err = azuritePool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "latest",
		Cmd:        []string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", AzuritePort},
		Name:       AzuriteContainerName,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.PortBindings = map[docker.Port][]docker.PortBinding{
			docker.Port(AzuritePort + "/tcp"): {{HostIP: "0.0.0.0", HostPort: AzuritePort}},
		}
	})
	if err != nil {
		return fmt.Errorf("cannot start Azurite container: %w", err)
	}

	// Wait until the port is ready.
	err = azuritePool.Retry(func() error {
		conn, derr := net.DialTimeout("tcp", AzuriteEndpoint, 2*time.Second)
		if derr != nil {
			return derr
		}
		_ = conn.Close()
		return nil
	})
	if err != nil {
		return err
	}

	// Note: Container creation is handled by azblob.NewFS through ensureContainer
	return nil
}

// CloseAzuriteContainer stops and removes the Azurite container used in integration tests.
func CloseAzuriteContainer() error {
	if azuritePool == nil || azuriteRes == nil {
		return nil
	}
	return azuritePool.Purge(azuriteRes)
}
