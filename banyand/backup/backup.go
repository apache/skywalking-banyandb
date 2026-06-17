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
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	cfg "github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/aws"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/azure"
	remoteconfig "github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/gcp"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/local"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	banyandbpath "github.com/apache/skywalking-banyandb/pkg/path"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// smallFileThreshold is the size below which files are uploaded concurrently.
// Backup snapshots are dominated by tiny index files whose upload cost is bound
// by per-request latency rather than bandwidth, so parallelizing them shortens
// the overall backup well within the schedule interval. Larger files are uploaded
// sequentially to keep the concurrent write-buffer memory bounded.
const smallFileThreshold = 5 << 20 // 5 MiB

type backupOptions struct {
	fsConfig          remoteconfig.FsConfig
	gRPCAddr          string
	cert              string
	timeStyle         string
	schedule          string
	streamRoot        string
	measureRoot       string
	propertyRoot      string
	traceRoot         string
	schemaRoot        string
	dest              string
	uploadConcurrency int
	enableTLS         bool
	insecure          bool
}

// NewBackupCommand creates a new backup command.
func NewBackupCommand() *cobra.Command {
	var backupOpts backupOptions
	// Initialize nested config structs to avoid nil dereference when
	// binding cobra flags below. This fixes a panic observed in tests when
	// accessing fields of FsConfig.S3/Azure before they were allocated.
	backupOpts.fsConfig.S3 = &remoteconfig.S3Config{}
	backupOpts.fsConfig.Azure = &remoteconfig.AzureConfig{}
	backupOpts.fsConfig.GCP = &remoteconfig.GCPConfig{}
	logging := logger.Logging{}
	cmd := &cobra.Command{
		Short:             "Backup BanyanDB snapshots to remote storage",
		DisableAutoGenTag: true,
		Version:           version.Build(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cfg.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			if err := logger.Init(logging); err != nil {
				return err
			}
			if backupOpts.schedule == "" {
				return backupAction(cmd.Context(), backupOpts)
			}
			schedLogger := logger.GetLogger().Named("backup-scheduler")
			schedLogger.Info().Msgf("backup to %s will run with schedule: %s", backupOpts.dest, backupOpts.schedule)
			clockInstance := clock.New()
			sch := timestamp.NewScheduler(schedLogger, clockInstance)
			// A full backup may legitimately run longer than the schedule interval.
			// The scheduler abandons (but does not cancel) an action that exceeds its
			// internal timeout, so without this guard a slow run would overlap with the
			// next scheduled run, stacking concurrent uploads until the process is
			// OOM-killed. backupInFlight ensures only one backup runs at a time: a tick
			// that fires while the previous run is still in progress is skipped.
			var backupInFlight atomic.Bool
			err := sch.Register(cmd.Context(), "backup", cron.Descriptor, backupOpts.schedule, func(ctx context.Context, _ time.Time, l *logger.Logger) bool {
				if !backupInFlight.CompareAndSwap(false, true) {
					l.Warn().Msg("previous backup is still running; skipping this scheduled run")
					return true
				}
				defer backupInFlight.Store(false)
				err := backupAction(ctx, backupOpts)
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
	cmd.Flags().StringVar(&backupOpts.traceRoot, "trace-root-path", "/tmp", "Root directory for trace catalog")
	cmd.Flags().StringVar(&backupOpts.schemaRoot, "schema-root-path", "/tmp", "Root directory for schema property catalog")
	cmd.Flags().StringVar(&backupOpts.dest, "dest", "", "Destination URL (e.g., file:///backups)")
	cmd.Flags().StringVar(&backupOpts.timeStyle, "time-style", "daily", "Time directory style (daily|hourly)")
	cmd.Flags().IntVar(&backupOpts.uploadConcurrency, "upload-concurrency", 8, "Number of concurrent uploads for small files (<5MiB)")
	cmd.Flags().StringVar(
		&backupOpts.schedule,
		"schedule",
		"",
		"Schedule expression for periodic backup. Options: @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>",
	)
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3.S3ConfigFilePath, "s3-config-file", "", "Path to the s3 configuration file")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3.S3CredentialFilePath, "s3-credential-file", "", "Path to the s3 credential file")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3.S3ProfileName, "s3-profile", "", "S3 profile name")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3.S3ChecksumAlgorithm, "s3-checksum-algorithm", "", "S3 checksum algorithm")
	cmd.Flags().StringVar(&backupOpts.fsConfig.S3.S3StorageClass, "s3-storage-class", "", "S3 upload storage class")
	// Azure flags
	cmd.Flags().StringVar(&backupOpts.fsConfig.Azure.AzureAccountName, "azure-account-name", "", "Azure storage account name")
	cmd.Flags().StringVar(&backupOpts.fsConfig.Azure.AzureAccountKey, "azure-account-key", "", "Azure storage account key")
	cmd.Flags().StringVar(&backupOpts.fsConfig.Azure.AzureSASToken, "azure-sas-token", "", "Azure SAS token (alternative to account key)")
	cmd.Flags().StringVar(&backupOpts.fsConfig.Azure.AzureEndpoint, "azure-endpoint", "", "Azure blob service endpoint")
	// GCP flags
	cmd.Flags().StringVar(&backupOpts.fsConfig.GCP.GCPServiceAccountFile, "gcp-service-account-file", "", "Path to the GCP service account JSON file")
	return cmd
}

func backupAction(ctx context.Context, options backupOptions) error {
	if options.dest == "" {
		return errors.New("dest is required")
	}
	var err error
	options.streamRoot, err = banyandbpath.Get(options.streamRoot)
	if err != nil {
		return err
	}
	options.measureRoot, err = banyandbpath.Get(options.measureRoot)
	if err != nil {
		return err
	}
	options.propertyRoot, err = banyandbpath.Get(options.propertyRoot)
	if err != nil {
		return err
	}
	options.traceRoot, err = banyandbpath.Get(options.traceRoot)
	if err != nil {
		return err
	}

	//nolint:contextcheck // Remote filesystem constructors are configuration-only and do not accept contexts.
	fs, err := newFS(options.dest, &options.fsConfig)
	if err != nil {
		return err
	}
	defer fs.Close()

	snapshots, err := snapshot.Get(ctx, options.gRPCAddr, options.enableTLS, options.insecure, options.cert)
	if err != nil {
		return err
	}

	timeDir := getTimeDir(options.timeStyle)

	for _, snp := range snapshots {
		var snapshotDir string
		snapshotDir, err = snapshot.Dir(snp, options.streamRoot, options.measureRoot, options.propertyRoot, options.traceRoot, options.schemaRoot)
		if err != nil {
			logger.Warningf("Failed to get snapshot directory for %s: %v", snp.Name, err)
			continue
		}
		catalogName := snapshot.CatalogName(snp.Catalog)
		if strings.HasPrefix(snp.Name, snapshot.SchemaPropertyCatalogName+"/") {
			catalogName = snapshot.SchemaPropertyCatalogName
		}
		multierr.AppendInto(&err, backupSnapshot(ctx, fs, snapshotDir, catalogName, timeDir, options.uploadConcurrency))
	}
	return err
}

func newFS(dest string, config *remoteconfig.FsConfig) (remote.FS, error) {
	u, err := url.Parse(dest)
	if err != nil {
		return nil, fmt.Errorf("invalid dest URL: %w", err)
	}

	switch u.Scheme {
	case "file":
		return local.NewFS(u.Path)
	case "s3":
		return aws.NewFS(u.Path, config)
	case "azure":
		return azure.NewFS(u.Host+u.Path, config)
	case "gcs", "gs":
		return gcp.NewFS(u.Host+u.Path, config)
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

func backupSnapshot(ctx context.Context, fs remote.FS, snapshotDir, catalog, timeDir string, concurrency int) error {
	prefix := path.Join(timeDir, catalog)

	remoteFiles, err := fs.List(ctx, prefix+"/")
	if err != nil {
		return err
	}
	// Build a set of existing remote files for O(1) lookups. The local file list
	// is never materialized; instead the snapshot is walked file by file so memory
	// stays bounded by the remote file count rather than the (much larger) local
	// file count.
	remoteSet := make(map[string]struct{}, len(remoteFiles))
	for _, remoteFile := range remoteFiles {
		remoteSet[remoteFile] = struct{}{}
	}

	if concurrency < 1 {
		concurrency = 1
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	walkErr := filepath.Walk(snapshotDir, func(filePath string, info os.FileInfo, iterErr error) error {
		if iterErr != nil {
			return iterErr
		}
		if info.IsDir() {
			return nil
		}
		// Stop walking promptly if a concurrent upload has already failed.
		if gctx.Err() != nil {
			return gctx.Err()
		}
		relPath, relErr := filepath.Rel(snapshotDir, filePath)
		if relErr != nil {
			return relErr
		}
		relPath = filepath.ToSlash(relPath)
		remotePath := path.Join(prefix, relPath)
		if _, ok := remoteSet[remotePath]; ok {
			// Present both locally and remotely: keep it and drop it from the
			// set so that whatever remains is exactly the orphaned remote files.
			delete(remoteSet, remotePath)
			return nil
		}
		if info.Size() < smallFileThreshold {
			// Small files dominate and are latency-bound: upload them concurrently.
			// relPath/remotePath are per-callback locals, so capturing them is safe.
			// g.Go blocks once the limit is reached, providing natural backpressure.
			g.Go(func() error {
				return uploadFile(gctx, fs, snapshotDir, relPath, remotePath)
			})
			return nil
		}
		// Large files are uploaded sequentially so at most one large write buffer
		// is held at a time, keeping peak memory bounded.
		return uploadFile(gctx, fs, snapshotDir, relPath, remotePath)
	})
	// Always wait for in-flight uploads before returning, even on a walk error.
	if waitErr := g.Wait(); waitErr != nil {
		return waitErr
	}
	if walkErr != nil {
		return walkErr
	}

	// Remaining entries exist remotely but no longer locally: delete them.
	for orphan := range remoteSet {
		if delErr := fs.Delete(ctx, orphan); delErr != nil {
			logger.Warningf("Warning: failed to delete orphaned file %s: %v\n", orphan, delErr)
		}
	}
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

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
