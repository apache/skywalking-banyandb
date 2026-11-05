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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

const (
	// conflictGroup and conflictStream name the stream the conflict case pre-creates on the
	// destination: the events report-events.sh reports, so the artifact always holds it.
	conflictGroup  = "sw_records"
	conflictStream = "event"
	// conflictTag is the tag the pre-created stream has on top of the artifact's definition.
	conflictTag = "import_e2e_extra"
)

// runConflictSchema writes, from the schema of artifact A, the definitions the conflict case
// creates on the destination with `bydbctl group|stream create -f`: <out dir>/group.json holds
// the conflictGroup group as exported, so it is identical to the artifact's, and
// <out dir>/stream.json the conflictStream stream with one extra string tag, a shape
// difference the import refuses unless a policy allows it. The revisions and timestamps the
// server assigns are cleared. It prints the two resource ids.
func runConflictSchema(args []string) int {
	if len(args) != 2 {
		usage()
	}
	dir, out := args[0], args[1]
	art, err := importer.OpenArtifact(context.Background(), dir, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	group, stream := art.Groups[conflictGroup], art.Streams[importer.ResourceKey{Group: conflictGroup, Name: conflictStream}]
	if group == nil || stream == nil || len(stream.GetTagFamilies()) == 0 {
		fmt.Fprintf(os.Stderr, "artifact %s has no group %s or stream %s/%s with tags\n", dir, conflictGroup, conflictGroup, conflictStream)
		return 1
	}
	group = importer.PrepareForWrite(group).(*commonv1.Group)
	group.Metadata.Id = 0
	stream = importer.PrepareForWrite(stream).(*databasev1.Stream)
	stream.Metadata.Id = 0
	family := stream.GetTagFamilies()[len(stream.GetTagFamilies())-1]
	family.Tags = append(family.Tags, &databasev1.TagSpec{Name: conflictTag, Type: databasev1.TagType_TAG_TYPE_STRING})
	if err = os.MkdirAll(out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for name, msg := range map[string]proto.Message{"group.json": group, "stream.json": stream} {
		raw, marshalErr := protojson.Marshal(msg)
		if marshalErr != nil {
			fmt.Fprintln(os.Stderr, marshalErr)
			return 1
		}
		if err = os.WriteFile(filepath.Join(out, name), raw, 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	fmt.Printf("group/%s\nstream/%s/%s\n", conflictGroup, conflictGroup, conflictStream)
	return 0
}
