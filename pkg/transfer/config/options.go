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

// Package config declares the settings of the data transfer commands and resolves them from
// flags, plan.yaml, BYDBCTL_* environment variables and defaults.
//
// Every setting is one struct field whose tags say where it comes from:
//
//	flag:"node-rate-limit"          the command-line flag (required); "-" declares a plan-only
//	                                setting: no flag, no variable, Sources keys it by plan path
//	short:"o"                       an optional one-letter shorthand
//	plan:"export.nodeRateLimit"     the dotted plan.yaml path; absent = not in plan.yaml
//	env:"NODE_RATE_LIMIT"           the variable BYDBCTL_NODE_RATE_LIMIT; absent = no variable
//	default:"0"                     the default, in the same grammar as the flag
//	usage:"..."                     the flag help text
//	parse:"rate-bytes"              the grammar: size, rate-bytes, rate-rows, parallelism,
//	                                count, selector, format, compress, conflict-policy or
//	                                conflict-rules (plan-only); string, bool, []string and
//	                                time.Duration fields need none
//
// Bind registers the flags and Resolve fills the struct with typed values, an explicit flag
// first, then plan.yaml, then the environment, then the default.
package config

import (
	"time"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// Connection is how a data command reaches the liaisons and logs.
//
//nolint:lll // one field per line keeps every source of a setting in one place
type Connection struct {
	Cert string `flag:"cert" plan:"connection.nodesTLS.cert" env:"CERT" usage:"Certificate for tls"`
	// LoggingLevel and LoggingEnv configure the client's own logger.
	LoggingLevel string   `flag:"logging-level" env:"LOGGING_LEVEL" default:"info" usage:"log level"`
	LoggingEnv   string   `flag:"logging-env" env:"LOGGING_ENV" default:"prod" usage:"log environment: prod|dev"`
	Nodes        []string `flag:"nodes" plan:"connection.nodes" env:"NODES" usage:"liaison gRPC addresses (host:17912); every usable one shares the export streams"`
	EnableTLS    bool     `flag:"enable-tls" plan:"connection.nodesTLS.enable" env:"ENABLE_TLS" usage:"Used to enable tls"`
	Insecure     bool     `flag:"insecure" plan:"connection.nodesTLS.insecure" env:"INSECURE" usage:"Used to skip server's cert"`
}

// Export is every setting of `data export`. The liaison rate limit is plan.yaml
// connection.rateLimit: it is a property of the liaisons, shared by export and import.
//
//nolint:lll // one field per line keeps every source of a setting in one place
type Export struct {
	Output       string                 `flag:"output" plan:"export.output" env:"OUTPUT" usage:"export directory; must not exist or be empty"`
	OutputFormat string                 `flag:"output-format" short:"o" env:"OUTPUT_FORMAT" default:"table" usage:"dry-run rendering: table|yaml|json"`
	Selectors    []*transferv1.Selector `flag:"selector" plan:"export.selectors" parse:"selector" usage:"scope, repeatable: catalog=<stream|measure|trace|property>[,groups=<g1>;<g2>]"`

	MaxFileSize uint64 `flag:"max-file-size" plan:"export.maxFileSize" env:"MAX_FILE_SIZE" default:"512MiB" parse:"size" usage:"soft size limit of one exported file on disk, 0 = unlimited"`

	NodeRateLimit    float64 `flag:"node-rate-limit" plan:"export.nodeRateLimit" env:"NODE_RATE_LIMIT" default:"0" parse:"rate-bytes" usage:"per data node byte rate, e.g. 50MiB/s or 3GiB/min; 0 = unlimited"`
	NodeRowRateLimit float64 `flag:"node-row-rate-limit" plan:"export.nodeRowRateLimit" env:"NODE_ROW_RATE_LIMIT" default:"0" parse:"rate-rows" usage:"per data node CSV row rate, e.g. 50000/s or 3000000/min; 0 = unlimited"`
	LiaisonRateLimit float64 `flag:"liaison-rate-limit" plan:"connection.rateLimit" env:"LIAISON_RATE_LIMIT" default:"0" parse:"rate-bytes" usage:"byte rate each liaison relays to this export, e.g. 100MiB/s; 0 = unlimited"`

	NodeIdleTimeout  time.Duration `flag:"node-idle-timeout" plan:"export.nodeIdleTimeout" env:"NODE_IDLE_TIMEOUT" default:"10m" usage:"fail a node stream that delivers nothing for this long; 0 disables"`
	SendRetryTimeout time.Duration `flag:"send-retry-timeout" plan:"export.sendRetryTimeout" env:"SEND_RETRY_TIMEOUT" default:"15m" usage:"how long to keep reopening an export stream refused as unavailable, busy, or by a liaison that cannot see the node, trying the next liaison each time"`

	// Parallelism is math.MaxInt for "max".
	Parallelism int                 `flag:"parallelism" plan:"export.parallelism" env:"PARALLELISM" default:"max" parse:"parallelism" usage:"number of data nodes exported concurrently: max or an integer >= 1"`
	Format      transferv1.Format   `flag:"format" plan:"export.format" env:"FORMAT" default:"csv" parse:"format" usage:"artifact format: csv (native is not implemented yet)"`
	Compress    transferv1.Compress `flag:"compress" plan:"export.compress" env:"COMPRESS" default:"gzip" parse:"compress" usage:"file compression: none|gzip"`

	CSVAnnotations bool `flag:"csv-annotations" plan:"export.csvAnnotations" env:"CSV_ANNOTATIONS" usage:"write a #datatype line before every CSV header"`
	IncludeSchema  bool `flag:"include-schema" plan:"export.includeSchema" env:"INCLUDE_SCHEMA" default:"true" usage:"export the schema too; false skips it and leaves groups[].indexMode unknown in the manifest"`
	SchemaOnly     bool `flag:"schema-only" plan:"export.schemaOnly" env:"SCHEMA_ONLY" usage:"export the schema and the manifest only, without a session or data"`
	Preempt        bool `flag:"preempt" env:"PREEMPT" usage:"take over any export session the cluster already holds"`
	DryRun         bool `flag:"dry-run" env:"DRY_RUN" usage:"inventory only: list units and estimated sizes without creating snapshots or writing files"`
	StrictCoverage bool `flag:"strict-coverage" env:"STRICT_COVERAGE" usage:"dry run: treat a data node that returned no inventory as an error (exit 2)"`
}

// Import is every setting of `data import`. The liaison rate limit is plan.yaml
// connection.rateLimit, as for an export.
//
//nolint:lll // one field per line keeps every source of a setting in one place
type Import struct {
	Input            string         `flag:"input" plan:"import.input" env:"IMPORT_INPUT" usage:"artifact directory written by data export"`
	OutputFormat     string         `flag:"output-format" short:"o" env:"OUTPUT_FORMAT" default:"table" usage:"rendering of the import plan: table|yaml|json"`
	OnSchemaConflict string         `flag:"on-schema-conflict" plan:"import.schemaConflictPolicy.default" env:"IMPORT_ON_SCHEMA_CONFLICT" default:"error" parse:"conflict-policy" usage:"default handling of a schema that differs from the target: error|ignore|overwrite; plan.yaml rules override it per resource"`
	ConflictRules    []ConflictRule `flag:"-" plan:"import.schemaConflictPolicy.rules" parse:"conflict-rules"`

	WriteBatchBytes  uint64  `flag:"write-batch-bytes" plan:"import.writeBatchBytes" env:"IMPORT_WRITE_BATCH_BYTES" default:"4MiB" parse:"size" usage:"bytes of rows sent in one batch, 64KiB to 64MiB"`
	WriteRateLimit   float64 `flag:"write-rate-limit" plan:"import.writeRateLimit" env:"IMPORT_WRITE_RATE_LIMIT" default:"0" parse:"rate-rows" usage:"rows written per second across the import, e.g. 50000/s or 3000000/min; 0 = unlimited"`
	LiaisonRateLimit float64 `flag:"liaison-rate-limit" plan:"connection.rateLimit" env:"LIAISON_RATE_LIMIT" default:"0" parse:"rate-bytes" usage:"byte rate sent to each liaison by this import, e.g. 100MiB/s; 0 = unlimited"`
	NodeRateLimit    float64 `flag:"node-rate-limit" plan:"import.nodeRateLimit" env:"IMPORT_NODE_RATE_LIMIT" default:"0" parse:"rate-bytes" usage:"byte rate each target data node admits from the imports, however many liaisons write to it, e.g. 50MiB/s or 3GiB/min; 0 = unlimited"`
	NodeRowRateLimit float64 `flag:"node-row-rate-limit" plan:"import.nodeRowRateLimit" env:"IMPORT_NODE_ROW_RATE_LIMIT" default:"0" parse:"rate-rows" usage:"row rate each target data node admits from the imports, however many liaisons write to it, e.g. 50000/s or 3000000/min; 0 = unlimited"`

	SendRetryTimeout time.Duration `flag:"send-retry-timeout" plan:"import.sendRetryTimeout" env:"IMPORT_SEND_RETRY_TIMEOUT" default:"15m" usage:"how long to keep reopening an import stream refused as unavailable or busy, trying the next liaison each time"`
	VerifyTimeout    time.Duration `flag:"verify-timeout" plan:"import.verifyTimeout" env:"IMPORT_VERIFY_TIMEOUT" default:"2m" usage:"how long --verify polls for the written rows to show up"`

	// Parallelism is math.MaxInt for "max".
	Parallelism    int `flag:"parallelism" plan:"import.parallelism" env:"IMPORT_PARALLELISM" default:"max" parse:"parallelism" usage:"number of units imported concurrently: max (the target data nodes) or an integer >= 1"`
	WriteBatchSize int `flag:"write-batch-size" plan:"import.writeBatchSize" env:"IMPORT_WRITE_BATCH_SIZE" default:"1000" parse:"count" usage:"rows sent in one batch, 1 to 10000"`
	// WriteInflightBatches is how many batches a worker keeps sent and not yet acknowledged
	// on one import stream.
	WriteInflightBatches int `flag:"write-inflight-batches" plan:"import.writeInflightBatches" env:"IMPORT_WRITE_INFLIGHT_BATCHES" default:"2" parse:"count" usage:"batches a worker keeps sent and not yet acknowledged per import stream, 1 to 8; a worker holds up to this many times --write-batch-bytes"`

	DryRun            bool `flag:"dry-run" env:"DRY_RUN" usage:"print the import plan and exit without writing anything: 0 = it would proceed, 2 = it would be refused"`
	IncludeSchema     bool `flag:"include-schema" plan:"import.includeSchema" env:"IMPORT_INCLUDE_SCHEMA" default:"true" usage:"compare and write the artifact's schema; false neither compares nor writes it"`
	SchemaOnly        bool `flag:"schema-only" plan:"import.schemaOnly" env:"IMPORT_SCHEMA_ONLY" usage:"write the schema only, without data"`
	Force             bool `flag:"force" usage:"confirm an overwrite schema conflict policy"`
	ForceTTL          bool `flag:"force-ttl" usage:"import units whose data the target TTL has already expired"`
	StrictCoverage    bool `flag:"strict-coverage" env:"IMPORT_STRICT_COVERAGE" usage:"refuse (exit 2) a non-property unit that would be imported from more than one source after picking the minimal covering set (or with --all-sources)"`
	AllSources        bool `flag:"all-sources" usage:"import every source of a unit instead of the minimal covering set"`
	AbortOnPlanChange bool `flag:"abort-on-plan-change" usage:"exit 5 when the plan re-checked after writing the schema differs from the first one"`
	Verify            bool `flag:"verify" plan:"import.verify" env:"IMPORT_VERIFY" default:"true" usage:"after the import, check the target's row counts against what was sent (exit 4 on a mismatch)"`
}

// planSchema lists every option struct whose plan tags make up plan.yaml. A plan file is
// checked against all of them, so one file serves every data command.
var planSchema = []any{Connection{}, Export{}, Import{}}
