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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = ginkgo.Describe("ChannelCloser", func() {
	var goods []gleak.Goroutine
	ginkgo.BeforeEach(func() {
		goods = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("ChannelCloser", func() {
		ginkgo.It("close simple channel", func() {
			var wg sync.WaitGroup

			chanL1 := make(chan struct{})
			workerNum := 10
			chanNum := 1
			wg.Add(workerNum + chanNum)

			chanCloser := NewChannelCloser()

			for i := 0; i < workerNum; i++ {
				go func(index int) {
					fmt.Printf("Start worker - %d\n", index)
					wg.Done()

					for {
						if chanCloser.AddSender() {
							time.Sleep(5 * time.Millisecond)
							chanL1 <- struct{}{}
							chanCloser.SenderDone()
						} else {
							fmt.Printf("Stop worker - %d\n", index)
							return
						}
					}
				}(i)
			}

			go func() {
				gomega.Expect(chanCloser.AddReceiver()).To(gomega.BeTrue())
				defer func() {
					fmt.Printf("Stop consumer: chanL1\n")
					chanCloser.ReceiverDone()
				}()

				fmt.Printf("Start consumer: chanL1\n")
				wg.Done()

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
			chanCloser.CloseThenWait()
			fmt.Printf("Stop close\n")
		})

		ginkgo.It("close multiple channels", func() {
			var wg sync.WaitGroup

			chanA := make(chan struct{})
			chanB := make(chan struct{})
			chanAWorkerNum := 10
			chanBWorkerNum := 10
			chanNum := 2
			wg.Add(chanAWorkerNum + chanBWorkerNum + chanNum)

			chanCloser := NewChannelCloser()

			for i := 0; i < chanAWorkerNum; i++ {
				go func(index int) {
					fmt.Printf("Start chan-a worker - %d\n", index)
					wg.Done()

					for {
						if chanCloser.AddSender() {
							time.Sleep(5 * time.Millisecond)
							chanA <- struct{}{}
							chanCloser.SenderDone()
						} else {
							fmt.Printf("Stop chan-a worker - %d\n", index)
							return
						}
					}
				}(i)
			}

			for i := 0; i < chanBWorkerNum; i++ {
				go func(index int) {
					fmt.Printf("Start chan-b worker - %d\n", index)
					wg.Done()

					for {
						if chanCloser.AddSender() {
							time.Sleep(5 * time.Millisecond)
							chanB <- struct{}{}
							chanCloser.SenderDone()
						} else {
							fmt.Printf("Stop chan-b worker - %d\n", index)
							return
						}
					}
				}(i)
			}

			go func() {
				gomega.Expect(chanCloser.AddReceiver()).To(gomega.BeTrue())
				defer func() {
					fmt.Printf("Stop consumer: chan-a\n")
					chanCloser.ReceiverDone()
				}()

				fmt.Printf("Start consumer: chan-a\n")
				wg.Done()

				for {
					select {
					case <-chanA:
						time.Sleep(10 * time.Millisecond)
					case <-chanCloser.CloseNotify():
						return
					}
				}
			}()

			go func() {
				gomega.Expect(chanCloser.AddReceiver()).To(gomega.BeTrue())
				defer func() {
					fmt.Printf("Stop consumer: chan-b\n")
					chanCloser.ReceiverDone()
				}()

				fmt.Printf("Start consumer: chan-b\n")
				wg.Done()

				for {
					select {
					case <-chanB:
						time.Sleep(10 * time.Millisecond)
					case <-chanCloser.CloseNotify():
						return
					}
				}
			}()

			wg.Wait()

			fmt.Printf("Start close...\n")

			chanCloser.CloseThenWait()

			fmt.Printf("Stop close\n")
		})
	})

	ginkgo.Context("ChannelGroupCloser", func() {
		ginkgo.It("close channel group", func() {
			var wg sync.WaitGroup

			chanL1 := make(chan struct{})
			chanL2 := make(chan struct{})
			workerNum := 10
			chanNum := 2
			wg.Add(workerNum + chanNum)

			chanL1Closer := NewChannelCloser()
			chanL2Closer := NewChannelCloser()
			channelGroupCloser := NewChannelGroupCloser(chanL1Closer, chanL2Closer)

			for i := 0; i < workerNum; i++ {
				go func(index int) {
					fmt.Printf("Start worker - %d\n", index)
					wg.Done()

					for {
						if chanL1Closer.AddSender() {
							time.Sleep(5 * time.Millisecond)
							chanL1 <- struct{}{}
							chanL1Closer.SenderDone()
						} else {
							fmt.Printf("Stop worker - %d\n", index)
							return
						}
					}
				}(i)
			}

			go func() {
				gomega.Expect(chanL1Closer.AddReceiver()).To(gomega.BeTrue())
				defer func() {
					fmt.Printf("Stop consumer: chanL1\n")
					chanL1Closer.ReceiverDone()
				}()

				fmt.Printf("Start consumer: chanL1\n")
				wg.Done()

				for {
					select {
					case req := <-chanL1:
						chanL2 <- req
					case <-chanL1Closer.CloseNotify():
						return
					}
				}
			}()

			go func() {
				gomega.Expect(chanL2Closer.AddReceiver()).To(gomega.BeTrue())
				defer func() {
					fmt.Printf("Stop consumer: chanL2\n")
					chanL2Closer.ReceiverDone()
				}()

				fmt.Printf("Start consumer: chanL2\n")
				wg.Done()

				for {
					select {
					case <-chanL2:
						time.Sleep(10 * time.Millisecond)
					case <-chanL2Closer.CloseNotify():
						return
					}
				}
			}()

			wg.Wait()

			fmt.Printf("Start close...\n")

			channelGroupCloser.CloseThenWait()

			fmt.Printf("Stop close\n")
		})
	})
})
