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

// Package auth provides configuration management and validation logic for authentication.
package auth

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Config AuthConfig.
type Config struct {
	Users             []User `yaml:"users"`
	Enabled           bool   `yaml:"-"`
	HealthAuthEnabled bool   `yaml:"-"`
}

// User details from config file.
type User struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// InitCfg returns Config with default values.
func InitCfg() *Config {
	return &Config{
		Enabled:           false,
		HealthAuthEnabled: false,
		Users:             []User{},
	}
}

// loadConfig implements the reading of the authentication configuration.
func (ar *Reloader) loadConfig(filePath string) error {
	if filePath == "" {
		return errors.New("configFile must be provided")
	}
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	perm := info.Mode().Perm()
	if perm != 0o600 {
		return fmt.Errorf("config file %s has unsafe permissions: %o (expected 0600)", filePath, perm)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	newCfg := InitCfg()
	err = yaml.Unmarshal(data, newCfg)
	if err != nil {
		return err
	}
	ar.setAuthEnabled(true)
	ar.setUsers(newCfg.Users)
	return nil
}

// Reloader manages dynamic reloading of auth config.
type Reloader struct {
	debounceTimer  *time.Timer
	updateCh       chan struct{}
	configFile     string
	Config         *Config
	watcher        *fsnotify.Watcher
	log            *logger.Logger
	lastConfigHash []byte
	mu             sync.RWMutex
}

// InitAuthReloader returns Reloader with default values.
func InitAuthReloader() *Reloader {
	return &Reloader{
		Config: InitCfg(),
	}
}

// ConfigAuthReloader returns a Reloader instance with properties populated.
func (ar *Reloader) ConfigAuthReloader(configFile string, healthAuthEnabled bool, log *logger.Logger) error {
	if configFile == "" {
		return errors.New("configFile must be provided")
	}
	if log == nil {
		return errors.New("logger must not be nil")
	}
	err := ar.loadConfig(configFile)
	if err != nil {
		return errors.Wrapf(err, "failed to load initial auth config from %s", configFile)
	}
	cfg := ar.GetConfig()
	ar.setHealthAuthEnabled(healthAuthEnabled)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "failed to create fsnotify watcher")
	}

	ar.setConfig(cfg)
	ar.configFile = configFile
	ar.log = log
	ar.watcher = watcher
	ar.updateCh = make(chan struct{}, 1)
	ar.lastConfigHash, _ = ar.computeFileHash(configFile)

	return nil
}

// Start begins monitoring the config file.
func (ar *Reloader) Start() error {
	if err := ar.watcher.Add(ar.configFile); err != nil {
		return errors.Wrapf(err, "failed to watch config file: %s", ar.configFile)
	}

	go ar.watchFiles()
	return nil
}

// Stop stops the watcher.
func (ar *Reloader) Stop() {
	_ = ar.watcher.Close()
}

// GetConfig returns the current config (safe for concurrent use).
func (ar *Reloader) GetConfig() *Config {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	return ar.Config
}

func (ar *Reloader) setConfig(cfg *Config) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.Config = cfg
}

func (ar *Reloader) setHealthAuthEnabled(enabled bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.Config.HealthAuthEnabled = enabled
}

func (ar *Reloader) setAuthEnabled(enabled bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.Config.Enabled = enabled
}

func (ar *Reloader) setUsers(users []User) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.Config.Users = users
}

// CheckUsernameAndPassword returns true if the provided username and password match any configured user.
func (ar *Reloader) CheckUsernameAndPassword(username, password string) bool {
	cfg := ar.GetConfig()

	username = strings.TrimSpace(username)
	password = strings.TrimSpace(password)

	for _, user := range cfg.Users {
		storedUsername := strings.TrimSpace(user.Username)
		storedPassword := strings.TrimSpace(user.Password)

		// Convert to []byte
		usernameBytes := []byte(username)
		storedUsernameBytes := []byte(storedUsername)
		passwordBytes := []byte(password)
		storedPasswordBytes := []byte(storedPassword)

		// Length must match
		if len(usernameBytes) != len(storedUsernameBytes) || len(passwordBytes) != len(storedPasswordBytes) {
			continue
		}

		// Use constant-time comparison
		usernameMatch := subtle.ConstantTimeCompare(usernameBytes, storedUsernameBytes) == 1
		passwordMatch := subtle.ConstantTimeCompare(passwordBytes, storedPasswordBytes) == 1

		if usernameMatch && passwordMatch {
			return true
		}
	}
	return false
}

// watchFiles listens for config changes.
func (ar *Reloader) watchFiles() {
	for {
		if ar.watcher == nil {
			ar.log.Error().Msg("watcher is nil, exiting watchFiles")
			return
		}
		select {
		case event, ok := <-ar.watcher.Events:
			if !ok {
				return
			}
			ar.log.Debug().Str("file", event.Name).Str("op", event.Op.String()).Msg("Detected auth file event")
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
				ar.scheduleReloadAttempt()
			}
		case err, ok := <-ar.watcher.Errors:
			if !ok {
				return
			}
			ar.log.Error().Err(err).Msg("watcher error")
		}
	}
}

// scheduleReloadAttempt debounces reload attempts.
func (ar *Reloader) scheduleReloadAttempt() {
	if ar.debounceTimer == nil {
		ar.debounceTimer = time.AfterFunc(500*time.Millisecond, ar.tryReload)
	} else {
		ar.debounceTimer.Reset(500 * time.Millisecond)
	}
}

// tryReload reloads config if changed.
func (ar *Reloader) tryReload() {
	changed, newHash, err := ar.checkContentChanged()
	if err != nil {
		ar.log.Error().Err(err).Msg("error checking config change")
		return
	}
	if !changed {
		return
	}

	err = ar.loadConfig(ar.configFile)
	if err != nil {
		ar.log.Error().Err(err).Msg("failed to reload config")
		return
	}

	ar.mu.Lock()
	ar.lastConfigHash = newHash
	ar.mu.Unlock()

	// notify
	select {
	case ar.updateCh <- struct{}{}:
	default:
	}
	ar.log.Info().Msg("auth config updated in memory")
}

// checkContentChanged compares file hash.
func (ar *Reloader) checkContentChanged() (bool, []byte, error) {
	currentHash, err := ar.computeFileHash(ar.configFile)
	if err != nil {
		return false, nil, err
	}
	return !bytes.Equal(ar.lastConfigHash, currentHash), currentHash, nil
}

// computeFileHash computes sha256 of file.
func (ar *Reloader) computeFileHash(filePath string) ([]byte, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	h.Write(content)
	return h.Sum(nil), nil
}

// GetUpdateChannel allows external consumers to watch for updates.
func (ar *Reloader) GetUpdateChannel() <-chan struct{} {
	return ar.updateCh
}
