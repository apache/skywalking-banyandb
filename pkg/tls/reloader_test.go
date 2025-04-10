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
	tempDir, err := os.MkdirTemp("", "tls-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
	err = os.WriteFile(certFile, certPEM1, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM1, 0o600)
	require.NoError(t, err)

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)

	defer reloader.Stop()

	initialCert, err := reloader.getCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(initialCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test1.local", leafCert.Subject.CommonName)

	certPEM2, keyPEM2 := generateSelfSignedCert(t, "test2.local")
	err = os.WriteFile(certFile, certPEM2, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM2, 0o600)
	require.NoError(t, err)

	err = reloader.reloadCertificate()
	require.NoError(t, err)

	updatedCert, err := reloader.getCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(updatedCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test2.local", leafCert.Subject.CommonName)

	tlsConfig := reloader.GetTLSConfig()
	configCert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(configCert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test2.local", leafCert.Subject.CommonName)
}

func TestReloader_FileOperations(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create initial files
	certPEM, keyPEM := generateSelfSignedCert(t, "initial.local")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	// Create reloader
	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	require.NoError(t, reloader.Start())
	defer reloader.Stop()

	// Test 1: Remove the old files and create new files
	t.Run("Remove and create new files", func(t *testing.T) {
		// Remove existing files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))
		time.Sleep(500 * time.Millisecond)

		// Create new files with different content
		certPEM, keyPEM = generateSelfSignedCert(t, "recreated.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
		time.Sleep(500 * time.Millisecond)

		// Force reload after file changes
		require.NoError(t, reloader.reloadCertificate())

		cert, err := reloader.getCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "recreated.local", leafCert.Subject.CommonName)
	})

	// Test 2: Create files after the reloader starts
	t.Run("Create files after reloader starts", func(t *testing.T) {
		// Remove existing files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))
		time.Sleep(500 * time.Millisecond)

		// Create new files with different content
		certPEM, keyPEM = generateSelfSignedCert(t, "newly.created.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
		time.Sleep(500 * time.Millisecond)

		// Force reload after file changes
		require.NoError(t, reloader.reloadCertificate())

		cert, err := reloader.getCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "newly.created.local", leafCert.Subject.CommonName)
	})

	// Test 3: Remove files and don't create new files
	t.Run("Remove files without recreation", func(t *testing.T) {
		// First ensure we have the correct certificate
		certPEM, keyPEM = generateSelfSignedCert(t, "newly.created.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, reloader.reloadCertificate())

		// Remove existing files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))
		time.Sleep(500 * time.Millisecond)

		// Should still return the last valid certificate
		cert, err := reloader.getCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "newly.created.local", leafCert.Subject.CommonName)
	})

	// Test 4: Create invalid files
	t.Run("Create invalid files", func(t *testing.T) {
		// First ensure we have a valid certificate
		certPEM, keyPEM = generateSelfSignedCert(t, "before.invalid.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, reloader.reloadCertificate())

		// Create invalid certificate and key files
		require.NoError(t, os.WriteFile(certFile, []byte("invalid cert"), 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte("invalid key"), 0o600))
		time.Sleep(500 * time.Millisecond)

		// Should still return the last valid certificate
		cert, err := reloader.getCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "before.invalid.local", leafCert.Subject.CommonName)

		// Test 5: Recover from invalid files
		t.Run("Recover from invalid files", func(t *testing.T) {
			// Create valid files after invalid ones
			certPEM, keyPEM = generateSelfSignedCert(t, "recovered.local")
			require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
			require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
			time.Sleep(500 * time.Millisecond)

			// Force reload after file changes
			require.NoError(t, reloader.reloadCertificate())

			cert, err := reloader.getCertificate(nil)
			require.NoError(t, err)
			leafCert, err := x509.ParseCertificate(cert.Certificate[0])
			require.NoError(t, err)
			assert.Equal(t, "recovered.local", leafCert.Subject.CommonName)
		})
	})
}
