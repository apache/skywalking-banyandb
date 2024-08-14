# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

QPS ?= 10

GROUP ?= "default"

.PHONY: up_traffic
up_traffic:
	curl -XPOST 'http://localhost:12800/mock-data/segments/tasks?qps=$(QPS)&group=$(GROUP)' -H'Content-Type: application/json' -d "@segment.tpl.json"

.PHONY: ls_traffic
ls_traffic:
	curl -XGET 'http://localhost:12800/mock-data/segments/tasks'

.PHONY: rm_traffic
rm_traffic:
	curl -XDELETE 'http://localhost:12800/mock-data/segments/tasks'