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

package grpc

import (
	"context"

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

type apiVersionService struct {
	commonv1.UnimplementedServiceServer
}

func (s *apiVersionService) GetAPIVersion(context.Context, *commonv1.GetAPIVersionRequest) (*commonv1.GetAPIVersionResponse, error) {
	return &commonv1.GetAPIVersionResponse{
		Version: &commonv1.APIVersion{
			Version:  apiversion.Version,
			Revision: apiversion.GetRevision(),
		},
	}, nil
}
