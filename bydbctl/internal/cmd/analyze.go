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
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
		RunE: func(_ *cobra.Command, args []string) (err error) {
			if len(args) == 0 {
				return errors.New("series index directory is required, its name should be 'sidx' in a segment 'seg-xxxxxx'")
			}
			store, err := inverted.NewStore(inverted.StoreOpts{
				Path:   args[0],
				Logger: logger.GetLogger("series-analyzer"),
			})
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			iter, err := store.SeriesIterator(ctx)
			if err != nil {
				return err
			}
			defer func() {
				err = multierr.Append(err, iter.Close())
			}()
			if subjectName != "" {
				var found bool
				for iter.Next() {
					var s pbv1.Series
					if err = s.Unmarshal(iter.Val().EntityValues); err != nil {
						return err
					}
					if s.Subject == subjectName {
						found = true
						for i := range s.EntityValues {
							fmt.Printf("%s,", pbv1.MustTagValueToStr(s.EntityValues[i]))
						}
						fmt.Println()
						continue
					}
					if found {
						break
					}
				}
				return
			}
			var subject string
			var count, total int
			for iter.Next() {
				total++
				var s pbv1.Series
				if err = s.Unmarshal(iter.Val().EntityValues); err != nil {
					return err
				}
				if s.Subject != subject {
					if subject != "" {
						fmt.Printf("%s, %d\n", subject, count)
					}
					subject = s.Subject
					count = 1
				} else {
					count++
				}
			}
			if subject != "" {
				fmt.Printf("%s, %d\n", subject, count)
			}
			fmt.Printf("total, %d\n", total)
			return nil
		},
	}

	seriesCmd.Flags().StringVarP(&subjectName, "subject", "s", "", "subject name")

	analyzeCmd.AddCommand(seriesCmd)
	return analyzeCmd
}
