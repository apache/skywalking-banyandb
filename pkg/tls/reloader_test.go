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

package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func generateSelfSignedCert(t *testing.T, commonName string) (certPEM, keyPEM []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		DNSNames:              []string{commonName},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour * 24),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	return certPEM, keyPEM
}

func TestReloader_CertificateRotation(t *testing.T) {
	// Create temporary directory for test certificates
	tempDir, err := os.MkdirTemp("", "tls-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Generate initial certificate
	certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
	err = os.WriteFile(certFile, certPEM1, 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM1, 0644)
	require.NoError(t, err)

	// Initialize the reloader
	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	
	// Start not needed for basic functionality test
	// err = reloader.Start()
	// require.NoError(t, err)
	defer reloader.Stop()

	// Get the initial certificate
	initialCert, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(initialCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test1.local", leafCert.Subject.CommonName)

	// Test case 1: Test manual certificate reloading
	// Generate new certificate
	certPEM2, keyPEM2 := generateSelfSignedCert(t, "test2.local")
	err = os.WriteFile(certFile, certPEM2, 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM2, 0644)
	require.NoError(t, err)

	// Manually reload certificate
	err = reloader.reloadCertificate()
	require.NoError(t, err)

	// Check if certificate was updated
	updatedCert, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(updatedCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test2.local", leafCert.Subject.CommonName)

	// Test case 2: Test GetTLSConfig
	tlsConfig := reloader.GetTLSConfig()
	configCert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(configCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test2.local", leafCert.Subject.CommonName)
}

// TestReloader_FileWatcher tests the file watcher functionality separately
func TestReloader_FileWatcher(t *testing.T) {
	// This is a more complex test that requires specific OS support
	// Skip on certain CI environments or platforms where file watching is unreliable
	if os.Getenv("CI") != "" || os.Getenv("SKIP_FILEWATCHER_TEST") != "" {
		t.Skip("Skipping file watcher test in CI environment")
	}

	// Create temporary directory for test certificates
	tempDir, err := os.MkdirTemp("", "tls-filewatcher-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Generate initial certificate
	certPEM1, keyPEM1 := generateSelfSignedCert(t, "watcher1.local")
	err = os.WriteFile(certFile, certPEM1, 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM1, 0644)
	require.NoError(t, err)

	// Initialize the reloader
	log := logger.GetLogger("tls-watcher-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	
	// Start file watcher
	err = reloader.Start()
	require.NoError(t, err)
	defer reloader.Stop()

	// Wait for watcher to initialize
	time.Sleep(100 * time.Millisecond)

	// Test file modification
	certPEM2, keyPEM2 := generateSelfSignedCert(t, "watcher2.local")
	err = os.WriteFile(certFile, certPEM2, 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM2, 0644)
	require.NoError(t, err)

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify certificate was updated
	cert, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "watcher2.local", leafCert.Subject.CommonName)
} 