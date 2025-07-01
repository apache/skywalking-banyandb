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

// Package backup provides the backup command-line tool.
package backup

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/aws"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/local"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

type backupOptions struct {
	fsConfig     remote.FsConfig
	gRPCAddr     string
	cert         string
	timeStyle    string
	schedule     string
	streamRoot   string
	measureRoot  string
	propertyRoot string
	dest         string
	enableTLS    bool
	insecure     bool
}

// NewBackupCommand creates a new backup command.
func NewBackupCommand() *cobra.Command {
	var backupOpts backupOptions
	logging := logger.Logging{}
	cmd := &cobra.Command{
		Short:             "Backup BanyanDB snapshots to remote storage",
		DisableAutoGenTag: true,
		Version:           version.Build(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			if err := logger.Init(logging); err != nil {
				return err
			}
			if backupOpts.schedule == "" {
				return backupAction(backupOpts)
			}
			schedLogger := logger.GetLogger().Named("backup-scheduler")
			schedLogger.Info().Msgf("backup to %s will run with schedule: %s", backupOpts.dest, backupOpts.schedule)
			clockInstance := clock.New()
			sch := timestamp.NewScheduler(schedLogger, clockInstance)
			err := sch.Register("backup", cron.Descriptor, backupOpts.schedule, func(_ time.Time, l *logger.Logger) bool {
				err := backupAction(backupOpts)
				if err != nil {
					l.Error().Err(err).Msg("backup failed")
				} else {
					l.Info().Msg("backup succeeded")
				}
				return true
			})
			if err != nil {
				return err
			}

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			schedLogger.Info().Msg("backup scheduler started, press Ctrl+C to exit")
			<-sigChan
			schedLogger.Info().Msg("shutting down backup scheduler...")
			sch.Close()
			return nil
		},
	}

	cmd.Flags().StringVar(&logging.Env, "logging-env", "prod", "the logging environment")
	cmd.Flags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	cmd.Flags().StringVar(&backupOpts.gRPCAddr, "grpc-addr", "127.0.0.1:17912", "gRPC address of the data node")
	cmd.Flags().BoolVar(&backupOpts.enableTLS, "enable-tls", false, "Enable TLS for gRPC connection")
	cmd.Flags().BoolVar(&backupOpts.insecure, "insecure", false, "Skip server certificate verification")
	cmd.Flags().StringVar(&backupOpts.cert, "cert", "", "Path to the gRPC server certificate")
	cmd.Flags().StringVar(&backupOpts.streamRoot, "stream-root-path", "/tmp", "Root directory for stream catalog")
	cmd.Flags().StringVar(&backupOpts.measureRoot, "measure-root-path", "/tmp", "Root directory for measure catalog")
	cmd.Flags().StringVar(&backupOpts.propertyRoot, "property-root-path", "/tmp", "Root directory for property catalog")
	cmd.Flags().StringVar(&backupOpts.dest, "dest", "", "Destination URL (e.g., file:///backups)")
	cmd.Flags().StringVar(&backupOpts.timeStyle, "time-style", "daily", "Time directory style (daily|hourly)")
	cmd.Flags().StringVar(
		&backupOpts.schedule,
		"schedule",
		"",
		"Schedule expression for periodic backup. Options: @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>",
	)
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3ConfigFilePath, "s3-config-file", "", "Path to the s3 configuration file")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3CredentialFilePath, "s3-credential-file", "", "Path to the s3 credential file")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3ProfileName, "s3-profile", "", "S3 profile name")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3ChecksumAlgorithm, "s3-checksum-algorithm", "", "S3 checksum algorithm")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3StorageClass, "s3-storage-class", "", "S3 upload storage class")
	return cmd
}

func backupAction(options backupOptions) error {
	if options.dest == "" {
		return errors.New("dest is required")
	}
	fs, err := newFS(options.dest, &options.fsConfig)
	if err != nil {
		return err
	}
	defer fs.Close()

	snapshots, err := snapshot.Get(options.gRPCAddr, options.enableTLS, options.insecure, options.cert)
	if err != nil {
		return err
	}

	timeDir := getTimeDir(options.timeStyle)

	for _, snp := range snapshots {
		var snapshotDir string
		snapshotDir, err = snapshot.Dir(snp, options.streamRoot, options.measureRoot, options.propertyRoot)
		if err != nil {
			logger.Warningf("Failed to get snapshot directory for %s: %v", snp.Name, err)
			continue
		}
		multierr.AppendInto(&err, backupSnapshot(fs, snapshotDir, snapshot.CatalogName(snp.Catalog), timeDir))
	}
	return err
}

func newFS(dest string, config *remote.FsConfig) (remote.FS, error) {
	u, err := url.Parse(dest)
	if err != nil {
		return nil, fmt.Errorf("invalid dest URL: %w", err)
	}

	switch u.Scheme {
	case "file":
		return local.NewFS(u.Path)
	case "s3":
		return aws.NewFS(u.Path, config)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}

func getTimeDir(style string) string {
	now := time.Now()
	switch style {
	case "hourly":
		return now.Format("2006-01-02-15")
	default:
		return now.Format("2006-01-02")
	}
}

func backupSnapshot(fs remote.FS, snapshotDir, catalog, timeDir string) error {
	localFiles, err := getAllFiles(snapshotDir)
	if err != nil {
		return err
	}

	ctx := context.Background()
	remotePrefix := path.Join(timeDir, catalog) + "/"

	remoteFiles, err := fs.List(ctx, remotePrefix)
	if err != nil {
		return err
	}
	for _, relPath := range localFiles {
		remotePath := path.Join(timeDir, catalog, relPath)
		if !contains(remoteFiles, remotePath) {
			if err := uploadFile(ctx, fs, snapshotDir, relPath, remotePath); err != nil {
				return err
			}
		}
	}

	deleteOrphanedFiles(ctx, fs, localFiles, remoteFiles, timeDir, catalog)
	return nil
}

func getAllFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(relPath))
		}
		return nil
	})
	return files, err
}

func uploadFile(ctx context.Context, fs remote.FS, snapshotDir, relPath, remotePath string) error {
	localPath := filepath.Join(snapshotDir, relPath)
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	return fs.Upload(ctx, remotePath, file)
}

func deleteOrphanedFiles(ctx context.Context, fs remote.FS, localFiles, remoteFiles []string, timeDir, snapshotName string) {
	expected := make(map[string]struct{})
	for _, f := range localFiles {
		expected[path.Join(timeDir, snapshotName, f)] = struct{}{}
	}

	for _, remoteFile := range remoteFiles {
		if _, exists := expected[remoteFile]; !exists {
			if err := fs.Delete(ctx, remoteFile); err != nil {
				logger.Warningf("Warning: failed to delete orphaned file %s: %v\n", remoteFile, err)
			}
		}
	}
}

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
