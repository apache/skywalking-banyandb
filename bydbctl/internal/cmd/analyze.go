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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newAnalyzeCmd() *cobra.Command {
	analyzeCmd := &cobra.Command{
		Use:     "analyze",
		Version: version.Build(),
		Short:   "Analyze operation",
	}

	var subjectName string
	seriesCmd := &cobra.Command{
		Use:     "series",
		Version: version.Build(),
		Short:   "Analyze series cardinality and distribution",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) == 0 {
				return errors.New("series index directory is required, its name should be 'sidx' in a segment 'seg-xxxxxx'")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), time.Minute)
			defer cancel()
			generation, openErr := native.OpenReadOnlyGeneration(args[0])
			if errors.Is(openErr, native.ErrNoSnapshot) {
				if subjectName == "" {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "total, 0")
				}
				return nil
			}
			if openErr != nil {
				return openErr
			}
			defer func() { err = errors.Join(err, generation.Close()) }()
			iter, iteratorErr := generation.NewSeriesIterator(ctx)
			if iteratorErr != nil {
				return iteratorErr
			}
			defer func() { err = errors.Join(err, iter.Close()) }()
			if subjectName != "" {
				var found bool
				for {
					value, nextErr := iter.Next()
					if nextErr != nil {
						return nextErr
					}
					if value == nil {
						break
					}
					var series pbv1.Series
					if unmarshalErr := series.Unmarshal(value); unmarshalErr != nil {
						return unmarshalErr
					}
					if series.Subject == subjectName {
						found = true
						for i := range series.EntityValues {
							fmt.Fprintf(cmd.OutOrStdout(), "%s,", pbv1.MustTagValueToStr(series.EntityValues[i]))
						}
						_, _ = fmt.Fprintln(cmd.OutOrStdout())
						continue
					}
					if found {
						break
					}
				}
				return nil
			}
			var subject string
			var count, total int
			for {
				value, nextErr := iter.Next()
				if nextErr != nil {
					return nextErr
				}
				if value == nil {
					break
				}
				total++
				var series pbv1.Series
				if unmarshalErr := series.Unmarshal(value); unmarshalErr != nil {
					return unmarshalErr
				}
				if series.Subject != subject {
					if subject != "" {
						fmt.Fprintf(cmd.OutOrStdout(), "%s, %d\n", subject, count)
					}
					subject, count = series.Subject, 1
				} else {
					count++
				}
			}
			if subject != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "%s, %d\n", subject, count)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "total, %d\n", total)
			return nil
		},
	}

	seriesCmd.Flags().StringVarP(&subjectName, "subject", "s", "", "subject name")

	analyzeCmd.AddCommand(seriesCmd)
	return analyzeCmd
}
