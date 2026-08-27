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
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Config AuthConfig.
type Config struct {
	Users             []User      `yaml:"users"`
	RBAC              RBACSection `yaml:"rbac"`
	Enabled           bool        `yaml:"-"`
	HealthAuthEnabled bool        `yaml:"-"`
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
	info, statErr := os.Stat(filePath)
	if statErr != nil {
		return statErr
	}
	perm := info.Mode().Perm()
	if perm != 0o600 {
		return fmt.Errorf("config file %s has unsafe permissions: %o (expected 0600)", filePath, perm)
	}

	data, readErr := os.ReadFile(filePath)
	if readErr != nil {
		return readErr
	}
	newCfg := InitCfg()
	if unmarshalErr := yaml.Unmarshal(data, newCfg); unmarshalErr != nil {
		return unmarshalErr
	}

	ar.mu.Lock()
	current := ar.snapshot.Load()
	revision := uint64(1)
	if current != nil {
		revision = current.Revision() + 1
	}
	snapshot, compileErr := compileSnapshot(revision, data)
	if compileErr != nil {
		ar.mu.Unlock()
		return compileErr
	}
	if ar.Config != nil {
		newCfg.HealthAuthEnabled = ar.Config.HealthAuthEnabled
	}
	newCfg.Enabled = true
	ar.Config = newCfg
	ar.snapshot.Store(snapshot)
	ar.mu.Unlock()
	return nil
}

// Reloader manages dynamic reloading of auth config.
type Reloader struct {
	debounceTimer  *time.Timer
	updateCh       chan struct{}
	Config         *Config
	watcher        *fsnotify.Watcher
	log            *logger.Logger
	snapshot       atomic.Pointer[compiledSnapshot]
	configFile     string
	lastConfigHash []byte
	mu             sync.RWMutex
}

// InitAuthReloader returns Reloader with default values.
func InitAuthReloader() *Reloader {
	reloader := &Reloader{
		Config: InitCfg(),
	}
	reloader.snapshot.Store(initialSnapshot)
	return reloader
}

// ConfigAuthReloader returns a Reloader instance with properties populated.
func (ar *Reloader) ConfigAuthReloader(configFile string, healthAuthEnabled bool, log *logger.Logger) error {
	if configFile == "" {
		return errors.New("configFile must be provided")
	}
	if log == nil {
		return errors.New("logger must not be nil")
	}
	if loadErr := ar.loadConfig(configFile); loadErr != nil {
		return errors.Wrapf(loadErr, "failed to load initial auth config from %s", configFile)
	}
	ar.setHealthAuthEnabled(healthAuthEnabled)

	watcher, watcherErr := fsnotify.NewWatcher()
	if watcherErr != nil {
		return errors.Wrap(watcherErr, "failed to create fsnotify watcher")
	}
	lastConfigHash, hashErr := ar.computeFileHash(configFile)
	if hashErr != nil {
		closeErr := watcher.Close()
		if closeErr != nil {
			return errors.Wrapf(hashErr, "failed to hash initial auth config from %s after closing watcher: %v", configFile, closeErr)
		}
		return errors.Wrapf(hashErr, "failed to hash initial auth config from %s", configFile)
	}

	ar.mu.Lock()
	ar.configFile = configFile
	ar.log = log
	ar.watcher = watcher
	ar.updateCh = make(chan struct{}, 1)
	ar.lastConfigHash = lastConfigHash
	ar.mu.Unlock()

	return nil
}

// Start begins monitoring the config file.
func (ar *Reloader) Start() error {
	if watchErr := ar.watcher.Add(ar.configFile); watchErr != nil {
		return errors.Wrapf(watchErr, "failed to watch config file: %s", ar.configFile)
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

func (ar *Reloader) setHealthAuthEnabled(enabled bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	if ar.Config == nil {
		ar.Config = InitCfg()
	}
	updated := *ar.Config
	updated.HealthAuthEnabled = enabled
	ar.Config = &updated
}

// CheckUsernameAndPassword returns true if the provided username and password match any configured user.
func (ar *Reloader) CheckUsernameAndPassword(username, password string) bool {
	_, authenticated := ar.CurrentSnapshot().Authenticate(username, password)
	return authenticated
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
	changed, newHash, changeErr := ar.checkContentChanged()
	if changeErr != nil {
		ar.log.Error().Err(changeErr).Msg("error checking config change")
		return
	}
	if !changed {
		return
	}

	if reloadErr := ar.loadConfig(ar.configFile); reloadErr != nil {
		ar.log.Error().Err(reloadErr).Msg("failed to reload config")
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
	currentHash, hashErr := ar.computeFileHash(ar.configFile)
	if hashErr != nil {
		return false, nil, hashErr
	}
	ar.mu.RLock()
	lastConfigHash := ar.lastConfigHash
	ar.mu.RUnlock()
	return !bytes.Equal(lastConfigHash, currentHash), currentHash, nil
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
