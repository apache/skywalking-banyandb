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

// Package tls provides common TLS utilities for HTTP and gRPC servers.
package tls

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Reloader manages dynamic reloading of TLS certificates and keys for servers.
//
//nolint:govet
type Reloader struct {
	cert          *tls.Certificate
	watcher       *fsnotify.Watcher
	log           *logger.Logger
	debounceTimer *time.Timer
	mu            sync.RWMutex
	lastCertHash  []byte
	lastKeyHash   []byte
	certFile      string
	keyFile       string
	updateCh      chan struct{}
}

// NewReloader creates a new TLSReloader instance.
func NewReloader(certFile, keyFile string, log *logger.Logger) (*Reloader, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("certFile and keyFile must be provided")
	}
	if log == nil {
		return nil, errors.New("logger must not be nil")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create fsnotify watcher")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		watcher.Close()
		return nil, errors.Wrap(err, "failed to load initial TLS certificate")
	}

	log.Info().Str("certFile", certFile).Str("keyFile", keyFile).Msg("Successfully loaded initial TLS certificates")

	tr := &Reloader{
		certFile: certFile,
		keyFile:  keyFile,
		cert:     &cert,
		log:      log,
		watcher:  watcher,
		updateCh: make(chan struct{}, 1),
	}

	// Compute initial hashes
	tr.lastCertHash, _ = tr.computeFileHash(certFile)
	if keyFile != "" {
		tr.lastKeyHash, _ = tr.computeFileHash(keyFile)
	}

	return tr, nil
}

// NewClientCertReloader creates a reloader that only monitors a CA certificate without requiring a key.
// This is useful for client-side certificate verification where only the CA cert is needed.
func NewClientCertReloader(certFile string, log *logger.Logger) (*Reloader, error) {
	if certFile == "" {
		return nil, errors.New("certFile must be provided")
	}
	if log == nil {
		return nil, errors.New("logger must not be nil")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create fsnotify watcher")
	}

	// Read the cert file content to ensure it exists and is valid
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		watcher.Close()
		return nil, errors.Wrap(err, "failed to read certificate file")
	}

	// Ensure the certificate is parsable
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		watcher.Close()
		return nil, errors.New("failed to parse PEM certificate")
	}

	log.Info().Str("certFile", certFile).Msg("Successfully loaded initial client certificate")

	tr := &Reloader{
		certFile: certFile,
		keyFile:  "", // No key file for client certs
		log:      log,
		watcher:  watcher,
		updateCh: make(chan struct{}, 1),
	}

	// Compute initial cert hash
	tr.lastCertHash, _ = tr.computeFileHash(certFile)

	return tr, nil
}

// Start begins monitoring the TLS certificate and key files for changes.
func (r *Reloader) Start() error {
	r.log.Info().Str("certFile", r.certFile).Str("keyFile", r.keyFile).Msg("Starting TLS file monitoring")

	err := r.watcher.Add(r.certFile)
	if err != nil {
		return errors.Wrapf(err, "failed to watch cert file: %s", r.certFile)
	}

	// Only add key file watcher if a key file was provided
	if r.keyFile != "" {
		err = r.watcher.Add(r.keyFile)
		if err != nil {
			return errors.Wrapf(err, "failed to watch key file: %s", r.keyFile)
		}
	}

	go r.watchFiles()

	return nil
}

// computeFileHash calculates a SHA-256 hash of a file's contents.
func (r *Reloader) computeFileHash(filePath string) ([]byte, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file for hashing: %s", filePath)
	}

	h := sha256.New()
	h.Write(content)
	return h.Sum(nil), nil
}

// scheduleReloadAttempt debounces reload attempts to avoid excessive reloads.
func (r *Reloader) scheduleReloadAttempt() {
	// Create or reset the debounce timer
	if r.debounceTimer == nil {
		r.debounceTimer = time.AfterFunc(500*time.Millisecond, func() {
			// Check if content has changed before reloading
			changed, newCertHash, newKeyHash, err := r.checkContentChanged()
			if err != nil {
				r.log.Error().Err(err).Msg("Error checking if certificate content changed")
				return
			}

			if !changed {
				r.log.Debug().Msg("Certificate content unchanged, skipping reload")
				return
			}

			// Content has changed, reload certificate
			if err := r.reloadCertificate(newCertHash, newKeyHash); err != nil {
				r.log.Error().Err(err).Msg("Failed to reload TLS certificate")
			} else {
				r.log.Info().Msg("Successfully updated TLS certificate after content change")
			}
		})
	} else {
		r.debounceTimer.Reset(500 * time.Millisecond)
	}
}

// checkContentChanged checks if file contents have changed and returns new hashes.
func (r *Reloader) checkContentChanged() (bool, []byte, []byte, error) {
	// Check if cert file has changed
	currentCertHash, err := r.computeFileHash(r.certFile)
	if err != nil {
		return false, nil, nil, errors.Wrap(err, "failed to compute current cert hash")
	}

	certChanged := !bytes.Equal(r.lastCertHash, currentCertHash)

	// If no key file, return just cert info
	if r.keyFile == "" {
		return certChanged, currentCertHash, nil, nil
	}

	// Check if key file has changed
	currentKeyHash, err := r.computeFileHash(r.keyFile)
	if err != nil {
		return false, nil, nil, errors.Wrap(err, "failed to compute current key hash")
	}

	keyChanged := !bytes.Equal(r.lastKeyHash, currentKeyHash)

	return certChanged || keyChanged, currentCertHash, currentKeyHash, nil
}

func (r *Reloader) watchFiles() {
	r.log.Info().Msg("TLS file watcher loop started")
	for {
		select {
		case event, ok := <-r.watcher.Events:
			if !ok {
				r.log.Info().Msg("Watcher events channel closed")
				return
			}

			r.log.Debug().Str("file", event.Name).Str("op", event.Op.String()).Msg("Detected file event")

			// Handle all relevant file operation events
			if event.Op&(fsnotify.Remove|fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				// Special handling for removal/creation
				if event.Op&(fsnotify.Remove|fsnotify.Create) != 0 {
					r.log.Info().Str("file", event.Name).Msg("File removed or created, waiting for stability")

					// Remove from watcher first to avoid duplicate watches
					_ = r.watcher.Remove(event.Name)

					// Wait for file operations to complete
					time.Sleep(200 * time.Millisecond)

					// Try to re-add files to watcher with retries
					maxRetries := 3
					for i := 0; i < maxRetries; i++ {
						if event.Name == r.certFile {
							if _, err := os.Stat(r.certFile); err == nil {
								if err := r.watcher.Add(r.certFile); err != nil {
									r.log.Error().Err(err).Str("file", r.certFile).Msg("Failed to re-add cert file to watcher")
								} else {
									r.log.Debug().Str("file", r.certFile).Msg("Re-added cert file to watcher")
									break
								}
							}
						} else if event.Name == r.keyFile {
							if _, err := os.Stat(r.keyFile); err == nil {
								if err := r.watcher.Add(r.keyFile); err != nil {
									r.log.Error().Err(err).Str("file", r.keyFile).Msg("Failed to re-add key file to watcher")
								} else {
									r.log.Debug().Str("file", r.keyFile).Msg("Re-added key file to watcher")
									break
								}
							}
						}
						if i < maxRetries-1 {
							time.Sleep(100 * time.Millisecond)
						}
					}
				} else {
					r.log.Info().Str("file", event.Name).Msg("Detected certificate modification")
					time.Sleep(200 * time.Millisecond) // Ensure file is fully written
				}

				// Schedule a reload attempt with debouncing for all types of events
				r.scheduleReloadAttempt()
			}

		case err, ok := <-r.watcher.Errors:
			if !ok {
				r.log.Info().Msg("Watcher errors channel closed")
				return
			}
			r.log.Error().Err(err).Msg("Error in file watcher")
		}
	}
}

// notifyUpdate sends update notification to the update channel.
func (r *Reloader) notifyUpdate() {
	select {
	case r.updateCh <- struct{}{}:
		r.log.Debug().Msg("Sent certificate update notification")
	default:
		r.log.Warn().Msg("Update channel is full, notification skipped")
	}
}

// reloadCertificate reloads the certificate from disk.
func (r *Reloader) reloadCertificate(newCertHash, newKeyHash []byte) error {
	r.log.Debug().Msg("Reloading TLS certificate")

	// For client certificates (no key file), just verify the certificate is valid
	if r.keyFile == "" {
		certPEM, err := os.ReadFile(r.certFile)
		if err != nil {
			return errors.Wrap(err, "failed to read certificate file")
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(certPEM) {
			return errors.New("failed to parse PEM certificate")
		}

		// Update the stored hash
		r.lastCertHash = newCertHash

		r.log.Debug().Msg("Client certificate updated in memory")
		r.notifyUpdate()
		return nil
	}

	// For server certificates with key files, load the key pair
	newCert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return errors.Wrap(err, "failed to reload TLS certificate")
	}

	// Update certificate and hashes
	r.mu.Lock()
	r.cert = &newCert
	r.lastCertHash = newCertHash
	r.lastKeyHash = newKeyHash
	r.mu.Unlock()

	r.log.Debug().Msg("TLS certificate updated in memory")
	r.notifyUpdate()
	return nil
}

// getCertificate returns the current certificate.
func (r *Reloader) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetUpdateChannel returns a channel that will be triggered when a certificate is updated.
func (r *Reloader) GetUpdateChannel() <-chan struct{} {
	return r.updateCh
}

// Stop gracefully stops the TLS reloader.
func (r *Reloader) Stop() {
	r.log.Info().Msg("Stopping TLS Reloader")

	// Stop the debounce timer if it exists
	if r.debounceTimer != nil {
		r.debounceTimer.Stop()
	}

	if err := r.watcher.Close(); err != nil {
		r.log.Error().Err(err).Msg("Failed to close fsnotify watcher")
	}
}

// GetTLSConfig returns a TLS config using this reloader's certificate.
func (r *Reloader) GetTLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: r.getCertificate,
		MinVersion:     tls.VersionTLS12,
		NextProtos:     []string{"h2"},
	}
}

// GetClientTLSConfig returns a TLS config for client-side certificate validation.
func (r *Reloader) GetClientTLSConfig(serverName string) (*tls.Config, error) {
	// Read the certificate file
	certPEM, err := os.ReadFile(r.certFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read certificate file")
	}

	// Create a certificate pool
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		return nil, errors.New("failed to parse PEM certificate")
	}

	// Create TLS config for client
	return &tls.Config{
		RootCAs:    certPool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}, nil
}
