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

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

// NewTimeDirCommand creates a new time-dir command.
func NewTimeDirCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "timedir",
		Short: "Manage 'time-dir' files for backup and restoration",
	}

	// Register subcommands.
	rootCmd.AddCommand(newListCmd())
	rootCmd.AddCommand(newCreateCmd())
	rootCmd.AddCommand(newReadCmd())
	rootCmd.AddCommand(newDeleteCmd())

	return rootCmd
}

func newListCmd() *cobra.Command {
	var dest string
	var prefix string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List remote time directories in the remote file system",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dest == "" {
				return errors.New("--dest is required")
			}

			// Create a remote file system client using the provided URL.
			cfg := new(remote.FsConfig)
			fs, err := newFS(dest, cfg)
			if err != nil {
				return err
			}

			ctx := context.Background()
			// List files starting with an optional prefix.
			files, err := fs.List(ctx, prefix)
			if err != nil {
				return fmt.Errorf("failed to list remote files: %w", err)
			}

			// Extract unique top-level directories (which are our time directories).
			dirSet := make(map[string]bool)
			for _, f := range files {
				// Normalize to forward-slash separators.
				normalized := filepath.ToSlash(f)
				parts := strings.SplitN(normalized, "/", 2)
				if len(parts) > 0 && parts[0] != "" {
					dirSet[parts[0]] = true
				}
			}
			var dirs []string
			for d := range dirSet {
				dirs = append(dirs, d)
			}
			sort.Strings(dirs)
			fmt.Fprintln(cmd.OutOrStdout(), "Remote time directories:")
			for _, d := range dirs {
				fmt.Fprintln(cmd.OutOrStdout(), d)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dest, "dest", "", "Destination URL of the remote file system (e.g., file:///backups)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "Prefix in the remote file system to list")
	return cmd
}

func newCreateCmd() *cobra.Command {
	var catalogs []string
	var streamRoot, measureRoot, propertyRoot string
	var timeStyle string

	cmd := &cobra.Command{
		Use:   "create [time]",
		Short: "Create local 'time-dir' file(s) in catalog directories",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(catalogs) == 0 {
				catalogs = []string{"stream", "measure", "property"}
			}
			var tValue string
			if len(args) > 0 {
				tValue = strings.TrimSpace(args[0])
			} else {
				tValue = getTimeDir(timeStyle)
			}

			for _, cat := range catalogs {
				filePath, err := getLocalTimeDirFilePath(cat, streamRoot, measureRoot, propertyRoot)
				if err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Skipping unknown catalog '%s': %v\n", cat, err)
					continue
				}
				dirPath := filepath.Dir(filePath)
				if err = os.MkdirAll(dirPath, storage.DirPerm); err != nil {
					return fmt.Errorf("failed to create time-dir directory: %w", err)
				}
				err = os.WriteFile(filePath, []byte(tValue), storage.FilePerm)
				if err != nil {
					return fmt.Errorf("failed to write time-dir file: %w", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Created time-dir for catalog '%s' at %s with content '%s'\n", cat, filePath, tValue)
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVar(&catalogs, "catalog", nil, "Catalog(s) to create time-dir file (e.g., stream, measure, property). Defaults to all if not provided.")
	cmd.Flags().StringVar(&streamRoot, "stream-root", "/tmp", "Local root directory for stream catalog")
	cmd.Flags().StringVar(&measureRoot, "measure-root", "/tmp", "Local root directory for measure catalog")
	cmd.Flags().StringVar(&propertyRoot, "property-root", "/tmp", "Local root directory for property catalog")
	cmd.Flags().StringVar(&timeStyle, "time-style", "daily", "Time style to compute time string (daily or hourly)")
	return cmd
}

func newReadCmd() *cobra.Command {
	var catalogs []string
	var streamRoot, measureRoot, propertyRoot string

	cmd := &cobra.Command{
		Use:   "read",
		Short: "Read local 'time-dir' file(s) from catalog directories",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// If no catalog is specified, process all three.
			if len(catalogs) == 0 {
				catalogs = []string{"stream", "measure", "property"}
			}

			for _, cat := range catalogs {
				filePath, err := getLocalTimeDirFilePath(cat, streamRoot, measureRoot, propertyRoot)
				if err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Skipping unknown catalog '%s': %v\n", cat, err)
					continue
				}
				data, err := os.ReadFile(filePath)
				if err != nil {
					if os.IsNotExist(err) {
						fmt.Fprintf(cmd.ErrOrStderr(), "Catalog '%s': time-dir file not found at %s\n", cat, filePath)
					} else {
						fmt.Fprintf(cmd.ErrOrStderr(), "Catalog '%s': failed to read time-dir file at %s: %v\n", cat, filePath, err)
					}
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Catalog '%s': time-dir content: '%s'\n", cat, strings.TrimSpace(string(data)))
				}
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVar(&catalogs, "catalog", nil, "Catalog(s) to read time-dir file (e.g., stream, measure, property). Defaults to all if not provided.")
	cmd.Flags().StringVar(&streamRoot, "stream-root", "/tmp", "Local root directory for stream catalog")
	cmd.Flags().StringVar(&measureRoot, "measure-root", "/tmp", "Local root directory for measure catalog")
	cmd.Flags().StringVar(&propertyRoot, "property-root", "/tmp", "Local root directory for property catalog")
	return cmd
}

func newDeleteCmd() *cobra.Command {
	var catalogs []string
	var streamRoot, measureRoot, propertyRoot string

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete local 'time-dir' file(s) from catalog directories",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// If no catalog is specified, process all three.
			if len(catalogs) == 0 {
				catalogs = []string{"stream", "measure", "property"}
			}
			for _, cat := range catalogs {
				filePath, err := getLocalTimeDirFilePath(cat, streamRoot, measureRoot, propertyRoot)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Skipping unknown catalog '%s': %v\n", cat, err)
					continue
				}
				err = os.Remove(filePath)
				if err != nil {
					if os.IsNotExist(err) {
						fmt.Fprintf(cmd.ErrOrStderr(), "Catalog '%s': time-dir file not found at %s\n", cat, filePath)
					} else {
						fmt.Fprintf(cmd.ErrOrStderr(), "Failed to delete time-dir for catalog '%s' at %s: %v\n", cat, filePath, err)
					}
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Deleted time-dir for catalog '%s' at %s\n", cat, filePath)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVar(&catalogs, "catalog", nil, "Catalog(s) to delete time-dir file (e.g., stream, measure, property). Defaults to all if not provided.")
	cmd.Flags().StringVar(&streamRoot, "stream-root", "/tmp", "Local root directory for stream catalog")
	cmd.Flags().StringVar(&measureRoot, "measure-root", "/tmp", "Local root directory for measure catalog")
	cmd.Flags().StringVar(&propertyRoot, "property-root", "/tmp", "Local root directory for property catalog")
	return cmd
}

func getLocalTimeDirFilePath(catalog, streamRoot, measureRoot, propertyRoot string) (string, error) {
	switch strings.ToLower(catalog) {
	case "stream":
		return filepath.Join(streamRoot, "stream", "time-dir"), nil
	case "measure":
		return filepath.Join(measureRoot, "measure", "time-dir"), nil
	case "property":
		return filepath.Join(propertyRoot, "property", "time-dir"), nil
	default:
		return "", fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
