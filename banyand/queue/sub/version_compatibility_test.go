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

package sub

import (
	"testing"

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

func TestCheckVersionCompatibility(t *testing.T) {
	serverAPIVersion := apiversion.Version
	serverFileFormatVersion := storage.GetCurrentVersion()
	compatibleVersions := storage.GetCompatibleVersions()

	tests := []struct {
		versionInfo     *clusterv1.VersionInfo
		name            string
		expectReason    string
		expectedStatus  modelv1.Status
		expectSupported bool
	}{
		{
			name:            "nil version info - should be compatible",
			versionInfo:     nil,
			expectedStatus:  modelv1.Status_STATUS_SUCCEED,
			expectSupported: true,
			expectReason:    "No version info provided, assuming compatible",
		},
		{
			name: "matching versions - should be compatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: serverFileFormatVersion,
			},
			expectedStatus:  modelv1.Status_STATUS_SUCCEED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
		{
			name: "mismatched API version - should be incompatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "0.8", // Different from server version
				FileFormatVersion: serverFileFormatVersion,
			},
			expectedStatus:  modelv1.Status_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version 0.8 not supported",
		},
		{
			name: "incompatible file format version - should be incompatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: "999.0.0", // Non-existent version
			},
			expectedStatus:  modelv1.Status_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "File format version 999.0.0 not compatible",
		},
		{
			name: "both API and file format incompatible - should be incompatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "0.8",
				FileFormatVersion: "999.0.0",
			},
			expectedStatus:  modelv1.Status_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version 0.8 not supported",
		},
		{
			name: "compatible file format from compatible list",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: getFirstCompatibleVersion(compatibleVersions, serverFileFormatVersion),
			},
			expectedStatus:  modelv1.Status_STATUS_SUCCEED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
		{
			name: "empty version strings - should be incompatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "",
				FileFormatVersion: "",
			},
			expectedStatus:  modelv1.Status_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version  not supported",
		},
		{
			name: "with compatible file format versions array",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:                  serverAPIVersion,
				FileFormatVersion:           serverFileFormatVersion,
				CompatibleFileFormatVersion: []string{"1.2.0", "1.1.0"},
			},
			expectedStatus:  modelv1.Status_STATUS_SUCCEED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compatibility, status := checkVersionCompatibility(tt.versionInfo)

			// Check status
			if status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}

			// Check supported flag
			if compatibility.Supported != tt.expectSupported {
				t.Errorf("expected supported %v, got %v", tt.expectSupported, compatibility.Supported)
			}

			// Check reason contains expected content
			if len(tt.expectReason) > 0 && len(compatibility.Reason) > 0 {
				if !contains(compatibility.Reason, tt.expectReason) {
					t.Errorf("expected reason to contain '%s', got '%s'", tt.expectReason, compatibility.Reason)
				}
			}

			// Check server version info is always populated
			if compatibility.ServerApiVersion != serverAPIVersion {
				t.Errorf("expected server API version %s, got %s", serverAPIVersion, compatibility.ServerApiVersion)
			}

			if compatibility.ServerFileFormatVersion != serverFileFormatVersion {
				t.Errorf("expected server file format version %s, got %s", serverFileFormatVersion, compatibility.ServerFileFormatVersion)
			}

			// Check supported versions are populated
			if len(compatibility.SupportedApiVersions) == 0 {
				t.Error("expected supported API versions to be populated")
			}

			if len(compatibility.SupportedFileFormatVersions) == 0 {
				t.Error("expected supported file format versions to be populated")
			}
		})
	}
}

func TestCheckSyncVersionCompatibility(t *testing.T) {
	serverAPIVersion := apiversion.Version
	serverFileFormatVersion := storage.GetCurrentVersion()
	compatibleVersions := storage.GetCompatibleVersions()

	tests := []struct {
		versionInfo     *clusterv1.VersionInfo
		name            string
		expectReason    string
		expectedStatus  clusterv1.SyncStatus
		expectSupported bool
	}{
		{
			name:            "nil version info - should be compatible",
			versionInfo:     nil,
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			expectSupported: true,
			expectReason:    "No version info provided, assuming compatible",
		},
		{
			name: "matching versions - should be compatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: serverFileFormatVersion,
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
		{
			name: "mismatched API version - should be version unsupported",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "0.8",
				FileFormatVersion: serverFileFormatVersion,
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version 0.8 not supported",
		},
		{
			name: "incompatible file format - should be format version mismatch",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: "999.0.0",
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_FORMAT_VERSION_MISMATCH,
			expectSupported: false,
			expectReason:    "File format version 999.0.0 not compatible",
		},
		{
			name: "both incompatible - should prioritize API version error",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "0.8",
				FileFormatVersion: "999.0.0",
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version 0.8 not supported",
		},
		{
			name: "compatible file format from list",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        serverAPIVersion,
				FileFormatVersion: getFirstCompatibleVersion(compatibleVersions, serverFileFormatVersion),
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
		{
			name: "empty version strings - should be incompatible",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:        "",
				FileFormatVersion: "",
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED,
			expectSupported: false,
			expectReason:    "API version  not supported",
		},
		{
			name: "with compatible file format versions array",
			versionInfo: &clusterv1.VersionInfo{
				ApiVersion:                  serverAPIVersion,
				FileFormatVersion:           serverFileFormatVersion,
				CompatibleFileFormatVersion: []string{"1.2.0", "1.1.0"},
			},
			expectedStatus:  clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			expectSupported: true,
			expectReason:    "Client version compatible with server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compatibility, status := checkSyncVersionCompatibility(tt.versionInfo)

			// Check status
			if status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}

			// Check supported flag
			if compatibility.Supported != tt.expectSupported {
				t.Errorf("expected supported %v, got %v", tt.expectSupported, compatibility.Supported)
			}

			// Check reason contains expected content
			if len(tt.expectReason) > 0 && len(compatibility.Reason) > 0 {
				if !contains(compatibility.Reason, tt.expectReason) {
					t.Errorf("expected reason to contain '%s', got '%s'", tt.expectReason, compatibility.Reason)
				}
			}

			// Check server version info is always populated
			if compatibility.ServerApiVersion != serverAPIVersion {
				t.Errorf("expected server API version %s, got %s", serverAPIVersion, compatibility.ServerApiVersion)
			}

			if compatibility.ServerFileFormatVersion != serverFileFormatVersion {
				t.Errorf("expected server file format version %s, got %s", serverFileFormatVersion, compatibility.ServerFileFormatVersion)
			}

			// Check supported versions are populated
			if len(compatibility.SupportedApiVersions) == 0 {
				t.Error("expected supported API versions to be populated")
			}

			if len(compatibility.SupportedFileFormatVersions) == 0 {
				t.Error("expected supported file format versions to be populated")
			}
		})
	}
}

// Helper function to get first compatible version that differs from current.
func getFirstCompatibleVersion(compatibleVersions []string, currentVersion string) string {
	for _, version := range compatibleVersions {
		if version != currentVersion {
			return version
		}
	}
	// If no different version found, return current version (still compatible)
	return currentVersion
}

// Helper function to check if a string contains a substring.
func contains(str, substr string) bool {
	return len(str) >= len(substr) && (str == substr ||
		(len(str) > len(substr) &&
			(findInString(str, substr) != -1)))
}

// Simple string search helper.
func findInString(str, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(str) < len(substr) {
		return -1
	}

	for i := 0; i <= len(str)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if str[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
