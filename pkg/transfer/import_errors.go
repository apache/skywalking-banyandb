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

package transfer

// The google.rpc.ErrorInfo detail an ImportRows stream ends with describes its failure with
// these domain, reasons and metadata keys.
const (
	// ImportErrorDomain is the ErrorInfo domain of every ImportService failure detail.
	ImportErrorDomain = "banyandb.transfer.import"

	// ImportReasonNodeFailed means a target node failed after the batch may have been partly
	// written to other nodes, so the client must not resend it.
	ImportReasonNodeFailed = "NODE_FAILED"
	// ImportReasonMessageTooLarge means a frame exceeded the liaison's receive message size.
	ImportReasonMessageTooLarge = "MESSAGE_TOO_LARGE"
	// ImportReasonRowInvalid means a row of the batch failed validation; nothing was written.
	ImportReasonRowInvalid = "ROW_INVALID"
	// ImportReasonSchemaNotFound means the group or the resource a row names does not exist.
	ImportReasonSchemaNotFound = "SCHEMA_NOT_FOUND"
	// ImportReasonNotWritten means the liaison refused before writing anything, on a failure
	// that may pass, such as a schema read; the client may resend the batch.
	ImportReasonNotWritten = "NOT_WRITTEN"

	// ImportRateBusy marks the message of a STATUS_SERVER_BUSY reply of a data node whose import
	// rate limit cannot admit a batch in time; the batch was not written and may be resent.
	ImportRateBusy = "import rate limit: "

	// ImportMetadataBatchSeq is the metadata key of the failed batch's batch_seq.
	ImportMetadataBatchSeq = "batch_seq"
	// ImportMetadataNode is the metadata key of the failed node's id.
	ImportMetadataNode = "node"
	// ImportMetadataRow is the metadata key of the failed row's index in its batch.
	ImportMetadataRow = "row"
)
