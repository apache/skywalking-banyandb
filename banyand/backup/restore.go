// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// NewRestoreCommand creates a new restore command.
func NewRestoreCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "Restore BanyanDB data from remote storage",
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return config.Load("logging", cmd.Flags())
		},
	}
	rootCmd.AddCommand(newRunCommand())
	rootCmd.AddCommand(NewTimeDirCommand())
	return rootCmd
}

func newRunCommand() *cobra.Command {
	var (
		source       string
		streamRoot   string
		measureRoot  string
		propertyRoot string
		fsConfig     remote.FsConfig
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Restore BanyanDB data from remote storage",
		RunE: func(_ *cobra.Command, _ []string) error {
			if streamRoot == "" && measureRoot == "" && propertyRoot == "" {
				return errors.New("at least one of stream-root-path, measure-root-path, or property-root-path is required")
			}
			if source == "" {
				return errors.New("source is required")
			}
			fs, err := newFS(source, &fsConfig)
			if err != nil {
				return err
			}
			defer fs.Close()

			var errs error

			if streamRoot != "" {
				timeDirPath := filepath.Join(streamRoot, "stream", "time-dir")
				if data, err := os.ReadFile(timeDirPath); err == nil {
					timeDir := strings.TrimSpace(string(data))
					if err = restoreCatalog(fs, timeDir, streamRoot, commonv1.Catalog_CATALOG_STREAM); err != nil {
						errs = multierr.Append(errs, fmt.Errorf("stream restore failed: %w", err))
					} else {
						_ = os.Remove(timeDirPath)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if measureRoot != "" {
				timeDirPath := filepath.Join(measureRoot, "measure", "time-dir")
				if data, err := os.ReadFile(timeDirPath); err == nil {
					timeDir := strings.TrimSpace(string(data))
					if err = restoreCatalog(fs, timeDir, measureRoot, commonv1.Catalog_CATALOG_MEASURE); err != nil {
						errs = multierr.Append(errs, fmt.Errorf("measure restore failed: %w", err))
					} else {
						_ = os.Remove(timeDirPath)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if propertyRoot != "" {
				timeDirPath := filepath.Join(propertyRoot, "property", "time-dir")
				if data, err := os.ReadFile(timeDirPath); err == nil {
					timeDir := strings.TrimSpace(string(data))
					if err = restoreCatalog(fs, timeDir, propertyRoot, commonv1.Catalog_CATALOG_PROPERTY); err != nil {
						errs = multierr.Append(errs, fmt.Errorf("property restore failed: %w", err))
					} else {
						_ = os.Remove(timeDirPath)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}

			return errs
		},
	}
	cmd.Flags().StringVar(&source, "source", "", "Source URL (e.g., file:///backups)")
	cmd.Flags().StringVar(&streamRoot, "stream-root-path", "/tmp", "Root directory for stream catalog")
	cmd.Flags().StringVar(&measureRoot, "measure-root-path", "/tmp", "Root directory for measure catalog")
	cmd.Flags().StringVar(&propertyRoot, "property-root-path", "/tmp", "Root directory for property catalog")
	cmd.Flags().StringVar(&fsConfig.S3ConfigFilePath, "s3-config-file", "", "Path to the s3 configuration file")
	cmd.Flags().StringVar(&fsConfig.S3CredentialFilePath, "s3-credential-file", "", "Path to the s3 credential file")
	cmd.Flags().StringVar(&fsConfig.S3ProfileName, "s3-profile", "", "S3 profile name")

	return cmd
}

func restoreCatalog(fs remote.FS, timeDir, rootPath string, catalog commonv1.Catalog) error {
	catalogName := snapshot.CatalogName(catalog)
	remotePrefix := filepath.Join(timeDir, catalogName, "/")

	remoteFiles, err := fs.List(context.Background(), remotePrefix)
	if err != nil {
		return fmt.Errorf("failed to list remote files: %w", err)
	}

	localDir := filepath.Join(snapshot.LocalDir(rootPath, catalog), storage.DataDir)
	if err = os.MkdirAll(localDir, storage.DirPerm); err != nil {
		return fmt.Errorf("failed to create local directory %s: %w", localDir, err)
	}

	logger.Infof("Restoring %s to %s from %s", catalogName, localDir, remotePrefix)

	remoteRelSet := make(map[string]bool)
	var relPath string
	for _, remoteFile := range remoteFiles {
		relPath, err = filepath.Rel(timeDir, remoteFile)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", remoteFile, err)
		}
		remoteRelSet[filepath.ToSlash(relPath)] = true
	}

	localFiles, err := getAllFiles(localDir)
	if err != nil {
		return fmt.Errorf("failed to list local files: %w", err)
	}

	for _, localRelPath := range localFiles {
		if !remoteRelSet[localRelPath] {
			localPath := filepath.Join(localDir, localRelPath)
			if err := os.Remove(localPath); err != nil {
				return fmt.Errorf("failed to remove local file %s: %w", localPath, err)
			}
			cleanEmptyDirs(filepath.Dir(localPath), localDir)
		}
	}

	for _, remoteFile := range remoteFiles {
		relPath, err := filepath.Rel(filepath.Join(timeDir, catalogName), remoteFile)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", remoteFile, err)
		}
		relPath = filepath.ToSlash(relPath)
		localPath := filepath.Join(rootPath, catalogName, storage.DataDir, relPath)

		if !contains(localFiles, relPath) {
			if err := os.MkdirAll(filepath.Dir(localPath), storage.DirPerm); err != nil {
				return fmt.Errorf("failed to create directory for %s: %w", localPath, err)
			}

			if err := downloadFile(context.Background(), fs, remoteFile, localPath); err != nil {
				return fmt.Errorf("failed to download %s: %w", remoteFile, err)
			}
			logger.Infof("Downloaded %s to %s", remoteFile, localPath)
		}
	}

	return nil
}

func cleanEmptyDirs(dir, stopDir string) {
	for {
		if dir == stopDir || dir == "." {
			break
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			break
		}
		_ = os.Remove(dir)
		dir = filepath.Dir(dir)
	}
}

func downloadFile(ctx context.Context, fs remote.FS, remotePath, localPath string) error {
	reader, err := fs.Download(ctx, remotePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return err
	}

	return nil
}
