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

package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"bufio"
	"time"
	"runtime"
	"testing"

	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"

	"github.com/stretchr/testify/require"
)

func mergeIntoWriter(parts []string, outputPath string) error {
    fOut, err := os.Create(outputPath)
    if err != nil {
        return err
    }
    defer fOut.Close()
    w := bufio.NewWriterSize(fOut, 4*1024)

    for _, part := range parts {
        fPart, err := os.Open(part)
        if err != nil {
            return err
        }
        _, err = bufio.NewReader(fPart).WriteTo(w)
        fPart.Close()
        if err != nil {
            return err
        }
    }
    // flush and sync
    if err := w.Flush(); err != nil {
        return err
    }
    return fOut.Sync()
}

// BenchmarkMergeOperations compares merge with and without fadvise
func BenchmarkMergeOperations(b *testing.B) {
    if runtime.GOOS != "linux" {
        b.Skip("fadvise optimization only supported on Linux")
    }

    testDir, cleanup := utils.SetupTestEnvironment(b)
    defer cleanup()

    parts := utils.CreateTestParts(b, testDir, 10, utils.LargeFileSize)
    scenarios := []struct{
        name   string
        enable bool
    }{
        {"WithoutFadvise", false},
        {"WithFadvise", true},
    }

    for _, sc := range scenarios {
        b.Run(sc.name, func(b *testing.B) {
            if sc.enable {
                utils.SetRealisticThreshold()
            } else {
                utils.SetTestThreshold(utils.Terabyte)
            }
            utils.WithMonitoringLegacy(b, func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    outputPath := filepath.Join(testDir, fmt.Sprintf("merged_%s_%d.dat", sc.name, i))
                    require.NoError(b, mergeIntoWriter(parts, outputPath))
                }
                b.StopTimer()
            })
            utils.CapturePageCacheStatsWithDelay(b, fmt.Sprintf("after_merge_%s", sc.name), 3)
            time.Sleep(50 * time.Millisecond)
        })
    }
}

// BenchmarkSequentialMergeOperations compares sequential triple merges with and without fadvise
func BenchmarkSequentialMergeOperations(b *testing.B) {
    if runtime.GOOS != "linux" {
        b.Skip("fadvise optimization only supported on Linux")
    }

    testDir, cleanup := utils.SetupTestEnvironment(b)
    defer cleanup()

    parts := utils.CreateTestParts(b, testDir, 5, utils.LargeFileSize)
    scenarios := []struct{
        name   string
        enable bool
    }{
        {"WithoutFadvise", false},
        {"WithFadvise", true},
    }

    for _, sc := range scenarios {
        b.Run(sc.name, func(b *testing.B) {
            if sc.enable {
                utils.SetRealisticThreshold()
            } else {
                utils.SetTestThreshold(utils.Terabyte)
            }
            utils.WithMonitoringLegacy(b, func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    for j := 0; j < 3; j++ {
                        outputPath := filepath.Join(testDir, fmt.Sprintf("seq_merged_%s_%d_%d.dat", sc.name, i, j))
                        require.NoError(b, mergeIntoWriter(parts, outputPath))
                    }
                }
                b.StopTimer()
            })
            utils.CapturePageCacheStatsWithDelay(b, fmt.Sprintf("after_seq_merge_%s", sc.name), 3)
            time.Sleep(50 * time.Millisecond)
        })
    }
}