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

	initialCert, err := reloader.GetCertificate(nil)
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

	updatedCert, err := reloader.GetCertificate(nil)
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

func TestReloader_FileWatcher(t *testing.T) {
	if os.Getenv("CI") != "" || os.Getenv("SKIP_FILEWATCHER_TEST") != "" {
		t.Skip("Skipping file watcher test in CI environment")
	}

	tempDir, err := os.MkdirTemp("", "tls-filewatcher-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	certPEM1, keyPEM1 := generateSelfSignedCert(t, "watcher1.local")
	err = os.WriteFile(certFile, certPEM1, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM1, 0o600)
	require.NoError(t, err)

	log := logger.GetLogger("tls-watcher-test")
	reloader, err := NewReloader(certFile, keyFile, log)
	require.NoError(t, err)

	err = reloader.Start()
	require.NoError(t, err)
	defer reloader.Stop()

	time.Sleep(100 * time.Millisecond)

	certPEM2, keyPEM2 := generateSelfSignedCert(t, "watcher2.local")
	err = os.WriteFile(certFile, certPEM2, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, keyPEM2, 0o600)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	cert, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	leafCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "watcher2.local", leafCert.Subject.CommonName)
}
