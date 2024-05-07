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

istio_mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
istio_mk_dir   := $(dir $(istio_mk_path))

root_dir := $(istio_mk_dir)../../..

include $(root_dir)/scripts/build/version.mk
include $(root_dir)/scripts/build/base.mk
include $(root_dir)/scripts/build/ginkgo.mk

.PHONY: profile
profile: $(GINKGO)
	$(GINKGO) -v --tags="prometheus" \
	  --blockprofile prof/block.prof \
	  --cpuprofile prof/cpu.prof \
	  --memprofile prof/mem.prof \
	  --mutexprofile prof/mutex.prof \
	  ./... 2 > $(root_dir)/test.log

.PHONY: test
test: $(GINKGO)
	$(GINKGO) -v --tags="prometheus" \
	  ./... 2 > $(root_dir)/test.log