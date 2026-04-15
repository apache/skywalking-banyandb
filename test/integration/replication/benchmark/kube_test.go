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

package benchmark

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverPodsAndService(t *testing.T) {
	podsJSON, err := os.ReadFile(filepath.Join("fixtures", "pods.json"))
	if err != nil {
		t.Fatalf("read pods fixture: %v", err)
	}
	servicesJSON, err := os.ReadFile(filepath.Join("fixtures", "services.json"))
	if err != nil {
		t.Fatalf("read services fixture: %v", err)
	}
	pods, err := parsePodsJSON(podsJSON)
	if err != nil {
		t.Fatalf("parse pods: %v", err)
	}
	services, err := parseServicesJSON(servicesJSON)
	if err != nil {
		t.Fatalf("parse services: %v", err)
	}
	dataPods := discoverDataPods(pods)
	if len(dataPods) != 2 {
		t.Fatalf("expected 2 data pods, got %d", len(dataPods))
	}
	liaisonPods := discoverLiaisonPods(pods)
	if len(liaisonPods) != 1 {
		t.Fatalf("expected 1 liaison pod, got %d", len(liaisonPods))
	}
	grpcSvc, err := discoverGRPCService(services)
	if err != nil {
		t.Fatalf("discover gRPC service: %v", err)
	}
	if grpcSvc.Metadata.Name != "banyandb-grpc" {
		t.Fatalf("unexpected service %s", grpcSvc.Metadata.Name)
	}
}

func TestDiscoverDataPodsExcludesLiaisonStatefulSet(t *testing.T) {
	pods := []kubePod{
		{
			Metadata: kubeMetadata{
				Name: "banyandb-data-0",
				Labels: map[string]string{
					"app.kubernetes.io/component": "data",
				},
				OwnerReferences: []kubeOwnerRef{{Kind: "StatefulSet", Name: "banyandb-data"}},
			},
		},
		{
			Metadata: kubeMetadata{
				Name: "banyandb-liaison-0",
				Labels: map[string]string{
					"app.kubernetes.io/component": "liaison",
				},
				OwnerReferences: []kubeOwnerRef{{Kind: "StatefulSet", Name: "banyandb-liaison"}},
			},
		},
	}

	dataPods := discoverDataPods(pods)
	if len(dataPods) != 1 {
		t.Fatalf("expected 1 data pod, got %d", len(dataPods))
	}
	if dataPods[0].Metadata.Name != "banyandb-data-0" {
		t.Fatalf("unexpected data pod %s", dataPods[0].Metadata.Name)
	}
}
