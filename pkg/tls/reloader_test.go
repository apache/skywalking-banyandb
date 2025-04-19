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
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// TestReloaderBasic tests the basic functionality of creating a TLS reloader.
func TestReloaderBasic(t *testing.T) {
	// Test with valid certificate and key files
	t.Run("valid certificate and key", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create valid certificate files
		certPEM, keyPEM, err := GenerateSelfSignedCert("test.local", []string{"test.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		require.NotNil(t, reloader)

		// Start the reloader
		require.NoError(t, reloader.Start())
		defer reloader.Stop()
	})

	// Test invalid inputs
	t.Run("error cases", func(t *testing.T) {
		log := logger.GetLogger("tls-test")

		// Empty paths
		reloader, err := NewReloader("", "", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)

		// Non-existent files
		reloader, err = NewReloader("nonexistent.pem", "nonexistent.pem", log)
		assert.Error(t, err)
		assert.Nil(t, reloader)

		// Nil logger
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")
		var certPEM, keyPEM []byte
		certPEM, keyPEM, err = GenerateSelfSignedCert("test.local", []string{"test.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		reloader, err = NewReloader(certFile, keyFile, nil)
		assert.Error(t, err)
		assert.Nil(t, reloader)
	})
}

// TestReloaderCertificateRotation tests the certificate rotation functionality.
func TestReloaderCertificateRotation(t *testing.T) {
	// Test updating existing files
	t.Run("updating existing files", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial certificate
		certPEM1, keyPEM1, err := GenerateSelfSignedCert("test1.local", []string{"test1.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM1, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM1, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		defer reloader.Stop()

		// Start reloader
		err = reloader.Start()
		require.NoError(t, err)

		// Wait for initial certificate to be loaded
		time.Sleep(100 * time.Millisecond)

		// Verify initial certificate
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "test1.local", leafCert.Subject.CommonName)

		// Generate and replace with new certificate files
		certPEM2, keyPEM2, err := GenerateSelfSignedCert("test2.local", []string{"test2.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

		// Use assert.Eventually instead of time.Sleep to wait for certificate update
		assert.Eventually(t, func() bool {
			cert, err := tlsConfig.GetCertificate(nil)
			if err != nil {
				return false
			}
			leafCert, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				return false
			}
			return leafCert.Subject.CommonName == "test2.local"
		}, flags.EventuallyTimeout, 100*time.Millisecond)
	})

	// Test removing old files and creating new files
	t.Run("removing and creating new files", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial certificate
		certPEM1, keyPEM1, err := GenerateSelfSignedCert("test1.local", []string{"test1.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM1, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM1, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		defer reloader.Stop()

		// Start reloader
		err = reloader.Start()
		require.NoError(t, err)

		// Wait for initial certificate to be loaded
		time.Sleep(100 * time.Millisecond)

		// Verify initial certificate
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "test1.local", leafCert.Subject.CommonName)

		// Remove the files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))
		time.Sleep(100 * time.Millisecond)

		// Create new files with different content
		certPEM2, keyPEM2, err := GenerateSelfSignedCert("test3.local", []string{"test3.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

		// Use assert.Eventually instead of time.Sleep to wait for certificate update
		assert.Eventually(t, func() bool {
			cert, err := tlsConfig.GetCertificate(nil)
			if err != nil {
				return false
			}
			leafCert, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				return false
			}
			return leafCert.Subject.CommonName == "test3.local"
		}, flags.EventuallyTimeout, 100*time.Millisecond)
	})

	// Test removing files without creating new ones
	t.Run("removing files without creating new ones", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create initial certificate
		certPEM, keyPEM, err := GenerateSelfSignedCert("remove-test.local", []string{"remove-test.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

		log := logger.GetLogger("tls-test")
		reloader, err := NewReloader(certFile, keyFile, log)
		require.NoError(t, err)
		defer reloader.Stop()

		// Start reloader
		err = reloader.Start()
		require.NoError(t, err)

		// Wait for initial certificate to be loaded
		time.Sleep(100 * time.Millisecond)

		// Get initial certificate
		tlsConfig := reloader.GetTLSConfig()
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "remove-test.local", leafCert.Subject.CommonName)

		// Remove the files
		require.NoError(t, os.Remove(certFile))
		require.NoError(t, os.Remove(keyFile))

		// Use assert.Eventually to check certificate is still available
		assert.Eventually(t, func() bool {
			cert, err := tlsConfig.GetCertificate(nil)
			if err != nil {
				return false
			}
			leafCert, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				return false
			}
			return leafCert.Subject.CommonName == "remove-test.local"
		}, flags.EventuallyTimeout, 100*time.Millisecond)
	})
}

// TestReloaderInvalidCertificate tests handling of invalid certificates.
func TestReloaderInvalidCertificate(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create valid initial files
	certPEM, keyPEM, err := GenerateSelfSignedCert("initial.local", []string{"initial.local"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)
	require.NoError(t, reloader.Start())
	defer reloader.Stop()

	// Wait for initial certificate to be loaded
	time.Sleep(100 * time.Millisecond)

	// Initial verification
	tlsConfig := reloader.GetTLSConfig()
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "initial.local", leafCert.Subject.CommonName)

	// Create invalid certificate and key files
	require.NoError(t, os.WriteFile(certFile, []byte("invalid cert"), 0o600))
	require.NoError(t, os.WriteFile(keyFile, []byte("invalid key"), 0o600))

	// Use assert.Eventually to verify still using the last valid certificate
	assert.Eventually(t, func() bool {
		cert, certErr := tlsConfig.GetCertificate(nil)
		if certErr != nil {
			return false
		}
		leafCert, leafErr := x509.ParseCertificate(cert.Certificate[0])
		if leafErr != nil {
			return false
		}
		return leafCert.Subject.CommonName == "initial.local"
	}, flags.EventuallyTimeout, 100*time.Millisecond)

	// Create valid files after invalid ones
	certPEM2, keyPEM2, err := GenerateSelfSignedCert("recovered.local", []string{"recovered.local"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

	// Use assert.Eventually to verify certificate has been updated
	assert.Eventually(t, func() bool {
		cert, certErr := tlsConfig.GetCertificate(nil)
		if certErr != nil {
			return false
		}
		leafCert, leafErr := x509.ParseCertificate(cert.Certificate[0])
		if leafErr != nil {
			return false
		}
		return leafCert.Subject.CommonName == "recovered.local"
	}, flags.EventuallyTimeout, 100*time.Millisecond)
}

// TestReloaderTLSConfig tests TLS configuration generation.
func TestReloaderTLSConfig(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Create valid files
	certPEM, keyPEM, err := GenerateSelfSignedCert("config.test.local", []string{"config.test.local"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := logger.GetLogger("tls-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)

	// Test TLS Config
	tlsConfig := reloader.GetTLSConfig()
	assert.NotNil(t, tlsConfig.GetCertificate)
	assert.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	assert.Equal(t, []string{"h2"}, tlsConfig.NextProtos)

	// Test gRPC Transport Credentials
	creds := credentials.NewTLS(tlsConfig)
	assert.NotNil(t, creds)
	info := creds.Info()
	assert.Equal(t, "tls", info.SecurityProtocol)
}

// TestClientCertReloader tests the client certificate reloader functionality.
func TestClientCertReloader(t *testing.T) {
	tempDir := t.TempDir()
	certFile := filepath.Join(tempDir, "cert.pem")

	// Create a self-signed CA certificate
	certPEM, _, err := GenerateSelfSignedCert("ca.test.local", []string{"ca.test.local"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))

	log := logger.GetLogger("tls-test")

	// Test basic initialization
	t.Run("initialization", func(t *testing.T) {
		reloader, err := NewClientCertReloader(certFile, log)
		require.NoError(t, err)
		require.NotNil(t, reloader)

		// Start the reloader
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Test GetClientTLSConfig
		clientConfig, err := reloader.GetClientTLSConfig("server.example.com")
		require.NoError(t, err)
		assert.NotNil(t, clientConfig.RootCAs)
		assert.Equal(t, "server.example.com", clientConfig.ServerName)
		assert.Equal(t, uint16(tls.VersionTLS12), clientConfig.MinVersion)
	})

	// Test certificate updates
	t.Run("certificate updates", func(t *testing.T) {
		reloader, err := NewClientCertReloader(certFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Get update channel
		updateCh := reloader.GetUpdateChannel()

		// Create a channel to notify when update is received
		updateReceived := make(chan struct{})
		go func() {
			<-updateCh
			close(updateReceived)
		}()

		// Allow time for watcher to be fully established
		time.Sleep(100 * time.Millisecond)

		// Replace the certificate
		certPEM2, _, err := GenerateSelfSignedCert("updated-ca.test.local", []string{"updated-ca.test.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))

		// Verify notification is received
		select {
		case <-updateReceived:
			// Notification received
		case <-time.After(flags.EventuallyTimeout):
			assert.Fail(t, "Timed out waiting for certificate update notification")
		}

		// Verify client gets new config
		updatedConfig, err := reloader.GetClientTLSConfig("server.example.com")
		require.NoError(t, err)
		assert.NotNil(t, updatedConfig.RootCAs)

		// Verify TLS config works correctly with the server name
		assert.Equal(t, "server.example.com", updatedConfig.ServerName)
	})

	// Test error conditions
	t.Run("error cases", func(t *testing.T) {
		// Empty path
		_, err := NewClientCertReloader("", log)
		assert.Error(t, err)

		// Invalid file content
		invalidCertFile := filepath.Join(tempDir, "invalid.pem")
		require.NoError(t, os.WriteFile(invalidCertFile, []byte("invalid certificate"), 0o600))
		_, err = NewClientCertReloader(invalidCertFile, log)
		assert.Error(t, err)

		// Nil logger
		_, err = NewClientCertReloader(certFile, nil)
		assert.Error(t, err)
	})

	// Test removing and recreating certificate file
	t.Run("remove and recreate certificate", func(t *testing.T) {
		reloader, err := NewClientCertReloader(certFile, log)
		require.NoError(t, err)
		require.NoError(t, reloader.Start())
		defer reloader.Stop()

		// Get initial client config
		initialConfig, err := reloader.GetClientTLSConfig("server.example.com")
		require.NoError(t, err)
		assert.NotNil(t, initialConfig.RootCAs)

		// Get update channel
		updateCh := reloader.GetUpdateChannel()
		updateReceived := make(chan struct{})
		go func() {
			<-updateCh
			close(updateReceived)
		}()

		// Allow time for watcher to be fully established
		time.Sleep(100 * time.Millisecond)

		// Remove certificate file
		require.NoError(t, os.Remove(certFile))

		// Wait a moment
		time.Sleep(100 * time.Millisecond)

		// Create a new certificate file
		certPEM3, _, err := GenerateSelfSignedCert("recreated-ca.test.local", []string{"recreated-ca.test.local"})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certFile, certPEM3, 0o600))

		// Verify update is received
		select {
		case <-updateReceived:
			// Notification received
		case <-time.After(flags.EventuallyTimeout):
			assert.Fail(t, "Timed out waiting for certificate update notification after recreation")
		}

		// Verify config works with new certificate
		time.Sleep(100 * time.Millisecond) // Give time for reloader to update
		assert.Eventually(t, func() bool {
			newConfig, err := reloader.GetClientTLSConfig("server.example.com")
			return err == nil && newConfig.RootCAs != nil
		}, flags.EventuallyTimeout, 100*time.Millisecond)
	})
}
