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

package run

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSimpleChannelCloser(_ *testing.T) {
	workerNum := 10
	var wg sync.WaitGroup
	wg.Add(workerNum + 1)

	chanL1 := make(chan struct{})
	chanCloser := NewChannelCloser(2)

	for i := 0; i < workerNum; i++ {
		go func(index int) {
			wg.Done()

			fmt.Printf("Start worker - %d\n", index)
			for {
				if chanCloser.AddRunning() {
					time.Sleep(5 * time.Millisecond)
					chanL1 <- struct{}{}
					chanCloser.RunningDone()
				} else {
					fmt.Printf("Stop worker - %d\n", index)
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Done()
		fmt.Printf("Start consumer: chanL1\n")

		defer func() {
			fmt.Printf("Stop consumer: chanL1\n")
			chanCloser.Done()
		}()

		for {
			select {
			case <-chanL1:
				time.Sleep(10 * time.Millisecond)
			case <-chanCloser.CloseNotify():
				return
			}
		}
	}()

	wg.Wait()

	fmt.Printf("Start close...\n")
	chanCloser.Done()
	chanCloser.CloseThenWait()
	fmt.Printf("Stop close\n")
}

func TestMultipleChannelCloser(_ *testing.T) {
	groupAWorkerNum := 10
	groupBWorkerNum := 10
	var wg sync.WaitGroup
	wg.Add(groupAWorkerNum + groupBWorkerNum + 2)

	chanL1 := make(chan struct{})
	chanL2 := make(chan struct{})
	chanCloser := NewChannelCloser(3)

	for i := 0; i < groupAWorkerNum; i++ {
		go func(index int) {
			wg.Done()

			fmt.Printf("Start group-a worker - %d\n", index)
			for {
				if chanCloser.AddRunning() {
					time.Sleep(5 * time.Millisecond)
					chanL1 <- struct{}{}
					chanCloser.RunningDone()
				} else {
					fmt.Printf("Stop worker - %d\n", index)
					return
				}
			}
		}(i)
	}

	for i := 0; i < groupBWorkerNum; i++ {
		go func(index int) {
			wg.Done()

			fmt.Printf("Start group-b worker - %d\n", index)
			for {
				if chanCloser.AddRunning() {
					time.Sleep(5 * time.Millisecond)
					chanL2 <- struct{}{}
					chanCloser.RunningDone()
				} else {
					fmt.Printf("Stop worker - %d\n", index)
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Done()

		fmt.Printf("Start consumer: chanL1\n")

		defer func() {
			fmt.Printf("Stop consumer: chanL1\n")
			chanCloser.Done()
		}()

		for {
			select {
			case <-chanL1:
				time.Sleep(10 * time.Millisecond)
			case <-chanCloser.CloseNotify():
				return
			}
		}
	}()

	go func() {
		wg.Done()

		fmt.Printf("Start consumer: chanL2\n")

		defer func() {
			fmt.Printf("Stop consumer: chanL2\n")
			chanCloser.Done()
		}()

		for {
			select {
			case <-chanL2:
				time.Sleep(10 * time.Millisecond)
			case <-chanCloser.CloseNotify():
				return
			}
		}
	}()

	wg.Wait()

	fmt.Printf("Start close...\n")
	chanCloser.Done()
	chanCloser.CloseThenWait()
	fmt.Printf("Stop close\n")
}

func TestAssociatedChannelCloser(_ *testing.T) {
	workerNum := 10
	var wg sync.WaitGroup
	wg.Add(workerNum + 2)

	chanL1 := make(chan struct{})
	chanL2 := make(chan struct{})
	chanCloser := NewChannelCloser(3)

	for i := 0; i < workerNum; i++ {
		go func(index int) {
			wg.Done()

			fmt.Printf("Start worker - %d\n", index)
			for {
				if chanCloser.AddRunning() {
					time.Sleep(5 * time.Millisecond)
					chanL1 <- struct{}{}
					chanCloser.RunningDone()
				} else {
					fmt.Printf("Stop worker - %d\n", index)
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Done()

		fmt.Printf("Start consumer: chanL1\n")

		defer func() {
			fmt.Printf("Stop consumer: chanL1\n")
			chanCloser.Done()
		}()

		for {
			select {
			case req := <-chanL1:

			ExitSendChan:
				for {
					select {
					case chanL2 <- req:
						// logical code
						break ExitSendChan
					default:
					}
					if chanCloser.Closed() {
						fmt.Printf("Discard unprocessed record: %v, consumer: chanL1\n", req)
						return
					}
					time.Sleep(10 * time.Millisecond)
				}

			case <-chanCloser.CloseNotify():
				return
			}
		}
	}()

	go func() {
		wg.Done()

		fmt.Printf("Start consumer: chanL2\n")

		defer func() {
			fmt.Printf("Stop consumer: chanL2\n")
			chanCloser.Done()
		}()

		for {
			select {
			case <-chanL2:
				time.Sleep(10 * time.Millisecond)
			case <-chanCloser.CloseNotify():
				return
			}
		}
	}()

	wg.Wait()

	fmt.Printf("Start close...\n")
	chanCloser.Done()
	chanCloser.CloseThenWait()
	fmt.Printf("Stop close\n")
}
