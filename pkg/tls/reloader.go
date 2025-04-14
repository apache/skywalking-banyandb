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
	"crypto/tls"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Reloader manages dynamic reloading of TLS certificates and keys for servers.
type Reloader struct {
	watcher  *fsnotify.Watcher
	cert     *tls.Certificate
	log      *logger.Logger
	certFile string
	keyFile  string
	mu       sync.RWMutex
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
	}

	return tr, nil
}

// Start begins monitoring the TLS certificate and key files for changes.
func (r *Reloader) Start() error {
	r.log.Info().Str("certFile", r.certFile).Str("keyFile", r.keyFile).Msg("Starting TLS file monitoring")

	err := r.watcher.Add(r.certFile)
	if err != nil {
		return errors.Wrapf(err, "failed to watch cert file: %s", r.certFile)
	}

	err = r.watcher.Add(r.keyFile)
	if err != nil {
		return errors.Wrapf(err, "failed to watch key file: %s", r.keyFile)
	}

	go r.watchFiles()

	return nil
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

			// Handle file removal and creation
			if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Create == fsnotify.Create {
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

				// Attempt to reload the certificate
				if err := r.reloadCertificate(); err != nil {
					r.log.Error().Err(err).Str("file", event.Name).Msg("Failed to reload TLS certificate")
				} else {
					r.log.Info().Str("file", event.Name).Msg("Successfully updated TLS certificate")
				}
				continue
			}

			// Handle file modifications
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Rename == fsnotify.Rename {
				r.log.Info().Str("file", event.Name).Msg("Detected certificate modification")

				time.Sleep(200 * time.Millisecond) // Increased delay to ensure file is fully written

				if err := r.reloadCertificate(); err != nil {
					r.log.Error().Err(err).Str("file", event.Name).Msg("Failed to reload TLS certificate")
				} else {
					r.log.Info().Str("file", event.Name).Msg("Successfully updated TLS certificate")
				}
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

// reloadCertificate reloads the certificate from disk.
func (r *Reloader) reloadCertificate() error {
	r.log.Debug().Msg("Attempting to reload TLS certificate")
	newCert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return errors.Wrap(err, "failed to reload TLS certificate")
	}

	r.mu.Lock()
	r.cert = &newCert
	r.mu.Unlock()

	r.log.Debug().Msg("TLS certificate updated in memory")
	return nil
}

// getCertificate returns the current certificate.
func (r *Reloader) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// Stop gracefully stops the TLS reloader.
func (r *Reloader) Stop() {
	r.log.Info().Msg("Stopping TLS Reloader")
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
