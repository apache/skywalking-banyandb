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
	"crypto/tls"
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
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "tls-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Generate initial certificate
	certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
	err = os.WriteFile(certFile, certPEM1, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM1, 0o600)
	require.NoError(t, err)

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	defer reloader.Stop()

	// Start reloader
	reloader.Start()

	// Wait for initial certificate to be loaded
	time.Sleep(100 * time.Millisecond)

	// Verify initial certificate
	tlsConfig := reloader.GetTLSConfig()
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "test1.local", leafCert.Subject.CommonName)

	// Create a new directory and move files there to trigger watcher
	newDir := filepath.Join(tempDir, "new")
	require.NoError(t, os.Mkdir(newDir, 0o755))

	// Move files to new location
	newCertFile := filepath.Join(newDir, "cert.pem")
	newKeyFile := filepath.Join(newDir, "key.pem")

	// Move files to trigger watcher events
	require.NoError(t, os.Rename(certFile, newCertFile))
	require.NoError(t, os.Rename(keyFile, newKeyFile))

	// Wait for watcher to detect changes
	time.Sleep(500 * time.Millisecond)

	// Verify certificate is still available (last known good state)
	cert, err = tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "test1.local", leafCert.Subject.CommonName)
}

func TestReloader_FileOperations(t *testing.T) {
	t.Run("Remove and create new files", func(t *testing.T) {
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

		// Create a new directory for the files
		newDir := filepath.Join(tempDir, "new")
		require.NoError(t, os.Mkdir(newDir, 0o755))

		// Move files to new location
		newCertFile := filepath.Join(newDir, "cert.pem")
		newKeyFile := filepath.Join(newDir, "key.pem")

		// Move files to trigger watcher events
		require.NoError(t, os.Rename(certFile, newCertFile))
		require.NoError(t, os.Rename(keyFile, newKeyFile))

		// Wait for watcher to detect changes
		time.Sleep(500 * time.Millisecond)

		// Verify certificate is still available (last known good state)
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "initial.local", leafCert.Subject.CommonName)
	})

	t.Run("Create files after reloader starts", func(t *testing.T) {
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

		// Create a new directory and move files there
		newDir := filepath.Join(tempDir, "new")
		require.NoError(t, os.Mkdir(newDir, 0o755))

		// Move files to new location
		newCertFile := filepath.Join(newDir, "cert.pem")
		newKeyFile := filepath.Join(newDir, "key.pem")

		// Move files to trigger watcher events
		require.NoError(t, os.Rename(certFile, newCertFile))
		require.NoError(t, os.Rename(keyFile, newKeyFile))

		// Wait for watcher to detect changes
		time.Sleep(500 * time.Millisecond)

		// Verify certificate is still available (last known good state)
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "initial.local", leafCert.Subject.CommonName)
	})

	t.Run("Remove files without recreation", func(t *testing.T) {
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

		// Remove files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))

		// Wait for file system events to be processed
		time.Sleep(500 * time.Millisecond)

		// Verify certificate is still available (last known good state)
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "initial.local", leafCert.Subject.CommonName)
	})

	t.Run("Create invalid files", func(t *testing.T) {
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

		// First ensure valid certificate is loaded through watcher
		certPEM2, keyPEM2 := generateSelfSignedCert(t, "before.invalid.local")
		require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))
		time.Sleep(500 * time.Millisecond)

		// Verify certificate was updated
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "before.invalid.local", leafCert.Subject.CommonName)

		// Create invalid certificate and key files
		require.NoError(t, os.WriteFile(certFile, []byte("invalid cert"), 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte("invalid key"), 0o600))
		time.Sleep(500 * time.Millisecond)

		// Should still return the last valid certificate
		cert, err = tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err = x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "before.invalid.local", leafCert.Subject.CommonName)
	})

	t.Run("Recover from invalid files", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial files with an "invalid" certificate (it's actually valid, just to set up the test)
		certPEM, keyPEM := generateSelfSignedCert(t, "before.invalid.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		// Create reloader
		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Create invalid certificate and key files
		require.NoError(t, os.WriteFile(certFile, []byte("invalid cert"), 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte("invalid key"), 0o600))
		time.Sleep(500 * time.Millisecond)

		// Create valid files after invalid ones
		certPEM3, keyPEM3 := generateSelfSignedCert(t, "recovered.local")
		require.NoError(t, os.WriteFile(certFile, certPEM3, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM3, 0o600))
		time.Sleep(500 * time.Millisecond)

		// Certificate should be updated automatically
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "recovered.local", leafCert.Subject.CommonName)
	})
}

func TestNewReloader_Errors(t *testing.T) {
	t.Run("empty cert and key files", func(t *testing.T) {
		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader("", "", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)
		assert.Contains(t, err.Error(), "certFile and keyFile must be provided")
	})

	t.Run("empty cert file", func(t *testing.T) {
		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader("", "key.pem", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)
		assert.Contains(t, err.Error(), "certFile and keyFile must be provided")
	})

	t.Run("empty key file", func(t *testing.T) {
		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader("cert.pem", "", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)
		assert.Contains(t, err.Error(), "certFile and keyFile must be provided")
	})

	t.Run("nil logger", func(t *testing.T) {
		reloader, err := NewReloader("cert.pem", "key.pem", nil)
		assert.Error(t, err)
		assert.Nil(t, reloader)
		assert.Contains(t, err.Error(), "logger must not be nil")
	})

	t.Run("invalid certificate files", func(t *testing.T) {
		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader("nonexistent.pem", "nonexistent.pem", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)
		assert.Contains(t, err.Error(), "failed to load initial TLS certificate")
	})
}

func TestReloader_InvalidCertificates(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create initial valid files
	certPEM, keyPEM := generateSelfSignedCert(t, "initial.local")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	require.NoError(t, reloader.Start())
	defer reloader.Stop()

	// Verify initial certificate
	tlsConfig := reloader.GetTLSConfig()
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "initial.local", leafCert.Subject.CommonName)

	// Test invalid certificate content
	t.Run("invalid certificate content", func(t *testing.T) {
		// Write invalid content
		require.NoError(t, os.WriteFile(certFile, []byte("invalid cert"), 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte("invalid key"), 0o600))
		time.Sleep(500 * time.Millisecond)

		// Should still have the original valid certificate
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "initial.local", leafCert.Subject.CommonName)
	})
}

func TestReloader_TLSConfigOptions(t *testing.T) {
	// Test TLS Config
	t.Run("TLS Config Options", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "config.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)

		config := reloader.GetTLSConfig()
		assert.NotNil(t, config.GetCertificate)
		assert.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
		assert.Equal(t, []string{"h2"}, config.NextProtos)
	})

	// Test gRPC Transport Credentials
	t.Run("gRPC Transport Credentials", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "config.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)

		creds := reloader.GetGRPCTransportCredentials()
		assert.NotNil(t, creds)
		info := creds.Info()
		assert.Equal(t, "tls", info.SecurityProtocol)
	})
}

func TestReloader_WatcherErrors(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create initial valid files
	certPEM, keyPEM := generateSelfSignedCert(t, "watcher.test.local")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)

	// Test Start() with invalid paths
	t.Run("Start with invalid cert path", func(t *testing.T) {
		// Remove cert file to make the path invalid
		require.NoError(t, os.Remove(certFile))
		err := reloader.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to watch cert file")
	})

	// Recreate cert file for next test
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))

	t.Run("Start with invalid key path", func(t *testing.T) {
		// Remove key file to make the path invalid
		require.NoError(t, os.Remove(keyFile))
		err := reloader.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to watch key file")
	})
}

func TestReloader_WatcherChannelClose(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create initial valid files
	certPEM, keyPEM := generateSelfSignedCert(t, "channel.test.local")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	require.NoError(t, reloader.Start())

	// Force close the watcher to trigger channel closure
	reloader.Stop()

	// Get certificate should still work
	tlsConfig := reloader.GetTLSConfig()
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert)
}

func TestReloader_FileWatcherEvents(t *testing.T) {
	// Test different file events
	t.Run("Certificate rename event", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "events.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		newCertFile := filepath.Join(tempDir, "cert.pem.new")
		require.NoError(t, os.Rename(certFile, newCertFile))
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, os.Rename(newCertFile, certFile))
		time.Sleep(100 * time.Millisecond)

		// Verify certificate still works
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
	})

	t.Run("Key rename event", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "events.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		newKeyFile := filepath.Join(tempDir, "key.pem.new")
		require.NoError(t, os.Rename(keyFile, newKeyFile))
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, os.Rename(newKeyFile, keyFile))
		time.Sleep(100 * time.Millisecond)

		// Verify certificate still works
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
	})

	// Test file permission changes
	t.Run("Certificate chmod event", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "events.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		require.NoError(t, os.Chmod(certFile, 0o644))
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, os.Chmod(certFile, 0o600))
		time.Sleep(100 * time.Millisecond)

		// Verify certificate still works
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
	})
}

func TestReloader_WatcherErrorHandling(t *testing.T) {
	t.Run("filesystem_errors", func(t *testing.T) {
		dir := t.TempDir()
		certPath := filepath.Join(dir, "cert.pem")
		keyPath := filepath.Join(dir, "key.pem")

		// Generate initial valid certificates
		cert, key := generateSelfSignedCert(t, "watcher.test.local")
		require.NoError(t, os.WriteFile(certPath, cert, 0o600))
		require.NoError(t, os.WriteFile(keyPath, key, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certPath, keyPath, log)
		require.NoError(t, err)

		// Start the reloader and wait for initial load
		err = reloader.Start()
		require.NoError(t, err)
		defer reloader.Stop()
		time.Sleep(100 * time.Millisecond)

		// Get initial certificate state
		initialConfig := reloader.GetTLSConfig()
		initialCert, err := initialConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, initialCert)

		// Create a directory with same name as cert file to trigger error
		require.NoError(t, os.Remove(certPath))
		require.NoError(t, os.Mkdir(certPath, 0o755))

		// Wait for watcher to detect the change
		time.Sleep(200 * time.Millisecond)

		// Verify the original certificate is still in use
		currentConfig := reloader.GetTLSConfig()
		currentCert, err := currentConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, currentCert)
		require.Equal(t, initialCert.Certificate, currentCert.Certificate)

		// Clean up and restore valid cert
		require.NoError(t, os.RemoveAll(certPath))
		require.NoError(t, os.WriteFile(certPath, cert, 0o600))
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("graceful_shutdown", func(t *testing.T) {
		dir := t.TempDir()
		certPath := filepath.Join(dir, "cert.pem")
		keyPath := filepath.Join(dir, "key.pem")

		// Generate initial valid certificates
		cert, key := generateSelfSignedCert(t, "watcher.test.local")
		require.NoError(t, os.WriteFile(certPath, cert, 0o600))
		require.NoError(t, os.WriteFile(keyPath, key, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certPath, keyPath, log)
		require.NoError(t, err)

		// Start the reloader and wait for initial load
		err = reloader.Start()
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		// Get initial config state
		config := reloader.GetTLSConfig()
		require.NotNil(t, config)
		initialCert, err := config.GetCertificate(nil)
		require.NoError(t, err)

		// Stop the reloader
		reloader.Stop()

		// Verify we can still get the TLS config and it has the same certificate
		newConfig := reloader.GetTLSConfig()
		require.NotNil(t, newConfig)
		newCert, err := newConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, initialCert.Certificate, newCert.Certificate)

		// Modify the cert file - should not trigger any updates
		newCertPEM, _ := generateSelfSignedCert(t, "modified.test.local")
		require.NoError(t, os.WriteFile(certPath, newCertPEM, 0o600))
		time.Sleep(100 * time.Millisecond)

		// Config should still have the original certificate
		finalConfig := reloader.GetTLSConfig()
		finalCert, err := finalConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, initialCert.Certificate, finalCert.Certificate)
	})
}

func TestReloader_CertificateRotationWithNewFiles(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create initial files
	certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
	require.NoError(t, os.WriteFile(certFile, certPEM1, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM1, 0o600))

	// Create reloader
	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	require.NoError(t, reloader.Start())
	defer reloader.Stop()

	// Wait for initial certificate to be loaded
	time.Sleep(100 * time.Millisecond)

	// Verify initial certificate
	tlsConfig := reloader.GetTLSConfig()
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test1.local", leafCert.Subject.CommonName)

	// Create new certificate and key files
	certPEM2, keyPEM2 := generateSelfSignedCert(t, "test2.local")
	require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

	// Wait for reloader to detect and process changes
	// This is longer than the sleep in the implementation to ensure all events are processed
	time.Sleep(500 * time.Millisecond)

	// Verify certificate was updated automatically
	cert, err = tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err = x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "test2.local", leafCert.Subject.CommonName)
}

func TestReloader_WatchFilesErrorPaths(t *testing.T) {
	// Test certificate file removal with failed re-add
	t.Run("cert_file_removal_with_failed_readd", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "watchfiles.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Get initial state
		initialConfig := reloader.GetTLSConfig()
		initialCert, err := initialConfig.GetCertificate(nil)
		require.NoError(t, err)

		// Remove the certificate file
		require.NoError(t, os.Remove(certFile))

		// Wait a bit for the watcher to detect removal
		time.Sleep(100 * time.Millisecond)

		// Create a directory with the same name as the certificate file
		// This will cause the re-add to fail due to file type mismatch
		require.NoError(t, os.Mkdir(certFile, 0o755))

		// Wait for retry attempts to complete
		time.Sleep(500 * time.Millisecond)

		// Verify the original certificate is still available
		currentConfig := reloader.GetTLSConfig()
		currentCert, err := currentConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, initialCert.Certificate, currentCert.Certificate)

		// Clean up and restore valid cert file
		require.NoError(t, os.RemoveAll(certFile))
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		time.Sleep(300 * time.Millisecond)
	})

	// Test key file removal with failed re-add
	t.Run("key_file_removal_with_failed_readd", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "watchfiles.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Get initial state
		initialConfig := reloader.GetTLSConfig()
		initialCert, err := initialConfig.GetCertificate(nil)
		require.NoError(t, err)

		// Remove the key file
		require.NoError(t, os.Remove(keyFile))

		// Wait a bit for the watcher to detect removal
		time.Sleep(100 * time.Millisecond)

		// Create a directory with the same name as the key file
		// This will cause the re-add to fail due to file type mismatch
		require.NoError(t, os.Mkdir(keyFile, 0o755))

		// Wait for retry attempts to complete
		time.Sleep(500 * time.Millisecond)

		// Verify the original certificate is still available
		currentConfig := reloader.GetTLSConfig()
		currentCert, err := currentConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, initialCert.Certificate, currentCert.Certificate)

		// Clean up and restore valid key file
		require.NoError(t, os.RemoveAll(keyFile))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
		time.Sleep(300 * time.Millisecond)
	})

	// Test invalid certificate format after write event
	t.Run("invalid_cert_after_write", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "watchfiles.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Get initial state
		initialConfig := reloader.GetTLSConfig()
		initialCert, err := initialConfig.GetCertificate(nil)
		require.NoError(t, err)

		// Write invalid certificate data
		require.NoError(t, os.WriteFile(certFile, []byte("not a valid certificate"), 0o600))

		// Wait for watcher to detect and process the change
		time.Sleep(500 * time.Millisecond)

		// Verify the original certificate is still available
		currentConfig := reloader.GetTLSConfig()
		currentCert, err := currentConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, initialCert.Certificate, currentCert.Certificate)

		// Restore valid certificate
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		time.Sleep(300 * time.Millisecond)
	})

	// Test rename events
	t.Run("rename_events", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial valid files
		certPEM, keyPEM := generateSelfSignedCert(t, "watchfiles.test.local")
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		newCertFile := filepath.Join(tempDir, "renamed_cert.pem")

		// Rename the certificate file
		require.NoError(t, os.Rename(certFile, newCertFile))

		// Wait for watcher to detect the change
		time.Sleep(300 * time.Millisecond)

		// Rename back to original
		require.NoError(t, os.Rename(newCertFile, certFile))

		// Wait for watcher to detect and process the change
		time.Sleep(300 * time.Millisecond)

		// Verify certificate still works
		config := reloader.GetTLSConfig()
		cert, err := config.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
	})
}

func TestReloader_StopWithError(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "tls-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create cert files
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")
	certPEM, keyPEM := generateSelfSignedCert(t, "test.local")
	err = os.WriteFile(certFile, certPEM, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM, 0o600)
	require.NoError(t, err)

	// Create reloader
	log := logger.GetLogger("tls-test")

	// First test the normal Stop path
	t.Run("normal stop", func(t *testing.T) {
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)

		// Make sure Stop doesn't panic
		assert.NotPanics(t, func() {
			reloader.Stop()
		})
	})

	// Now test calling Stop on an already stopped reloader
	t.Run("double stop", func(t *testing.T) {
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)

		// First Stop call
		reloader.Stop()

		// Second Stop call should hit the error path but not panic
		assert.NotPanics(t, func() {
			reloader.Stop()
		})
	})
}
