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

// Package timeouts holds timeout constants shared between the FODC agent and proxy.
// Co-locating these constants ensures the proxy-side per-agent collection deadline is
// always strictly greater than the agent-side InspectAll timeout, regardless of who
// imports them.
package timeouts

import "time"

// AgentInspectAll bounds how long a single InspectAll call against the local liaison
// may run on the FODC agent side.
const AgentInspectAll = 40 * time.Second

// ProxySlack is the additional time the proxy waits, on top of AgentInspectAll, before
// declaring an agent unresponsive. It must be strictly greater than zero so the proxy
// always outlasts the agent's own deadline and never gives up while a slow but still
// progressing InspectAll call is in flight.
const ProxySlack = 10 * time.Second
