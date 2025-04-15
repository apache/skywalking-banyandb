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
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
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

// TestReloaderBasic tests the basic functionality of creating a TLS reloader.
func TestReloaderBasic(t *testing.T) {
	// Test with valid certificate and key files
	t.Run("valid certificate and key", func(t *testing.T) {
		tempDir := t.TempDir()
		certFile := filepath.Join(tempDir, "cert.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Create valid certificate files
		certPEM, keyPEM := generateSelfSignedCert(t, "test.local")
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
		certPEM, keyPEM := generateSelfSignedCert(t, "test.local")
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
		certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
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
		certPEM2, keyPEM2 := generateSelfSignedCert(t, "test2.local")
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
		certPEM1, keyPEM1 := generateSelfSignedCert(t, "test1.local")
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
		certPEM2, keyPEM2 := generateSelfSignedCert(t, "test3.local")
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
		certPEM, keyPEM := generateSelfSignedCert(t, "remove-test.local")
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
	certPEM, keyPEM := generateSelfSignedCert(t, "initial.local")
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
		cert, err := tlsConfig.GetCertificate(nil)
		if err != nil {
			return false
		}
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return false
		}
		return leafCert.Subject.CommonName == "initial.local"
	}, flags.EventuallyTimeout, 100*time.Millisecond)

	// Create valid files after invalid ones
	certPEM2, keyPEM2 := generateSelfSignedCert(t, "recovered.local")
	require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

	// Use assert.Eventually to verify certificate has been updated
	assert.Eventually(t, func() bool {
		cert, err := tlsConfig.GetCertificate(nil)
		if err != nil {
			return false
		}
		leafCert, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
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
	certPEM, keyPEM := generateSelfSignedCert(t, "config.test.local")
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
