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
	"time"
)

// GenerateSelfSignedCert creates a new self-signed certificate for testing purposes.
// It returns the certificate PEM, key PEM, and any error that occurred.
func GenerateSelfSignedCert(commonName string, extraDNSNames []string) (certPEM, keyPEM []byte, err error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	// Always include the commonName as a DNS name
	dnsNames := []string{commonName}

	// Add extra DNS names if provided
	if len(extraDNSNames) > 0 {
		dnsNames = append(dnsNames, extraDNSNames...)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		DNSNames:              dnsNames,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour * 24),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	return certPEM, keyPEM, nil
}
