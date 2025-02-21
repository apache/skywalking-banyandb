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

// Package banyand implements a executable database server.
package banyand

/*
                                                                  鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
                                                                  鈹?                    Storage                        鈹?
                   鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                            鈹?                                                   鈹?
                   鈹?               鈹?                            鈹?     鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?鈹?
                   鈹?   Discovery   鈹傗梽鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?     鈹?               鈹?        鈹?               鈹?鈹?
                   鈹?               鈹?                            鈹?  鈹屸攢鈻衡攤    Series      鈹溾攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈻衡攤                鈹?鈹?
                   鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                            鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
                           鈹?                                     鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
                           鈹?                                     鈹?  鈹? 鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹?               鈹?鈹?
                           鈹?                                     鈹?  鈹?        鈻?                  鈹?               鈹?鈹?
                           鈹?                                     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
                           鈻?                                     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
鈹?                                                         鈹?     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
鈹?   鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                                   鈹?     鈹?  鈹? 鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹?               鈹?鈹?
鈹?   鈹?               鈹?     鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?鈹?     鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
鈹?   鈹?   Endpoint    鈹溾攢鈹€鈹€鈹€鈹€鈻衡攤   Remote / Local Queue    鈹溾攢鈹尖攢鈹€鈹€鈹€鈹€鈹€鈹尖攢鈹€鈹€鈹尖攢鈹€鈹?   Query       鈹?        鈹?    KV Engine  鈹?鈹?
鈹?   鈹?               鈹?     鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?鈹?     鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
鈹?   鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                                   鈹?     鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
鈹?                               Liaison                   鈹?     鈹?  鈹? 鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹?               鈹?鈹?
鈹?                                                         鈹?     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?     鈹?  鈹?        鈹?                  鈹?               鈹?鈹?
                                                                  鈹?  鈹?        鈻?                  鈹?               鈹?鈹?
                                                                  鈹?  鈹? 鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹?               鈹?鈹?
                                                                  鈹?  鈹? 鈹?               鈹?        鈹?               鈹?鈹?
                                                                  鈹?  鈹? 鈹?   Index       鈹?        鈹?               鈹?鈹?
                                                                  鈹?  鈹斺攢鈻衡攤                鈹溾攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈻衡攤                鈹?鈹?
                                                                  鈹?     鈹?               鈹?        鈹?               鈹?鈹?
                                                                  鈹?     鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?鈹?
                                                                  鈹?                                                   鈹?
                                                                  鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
*/
