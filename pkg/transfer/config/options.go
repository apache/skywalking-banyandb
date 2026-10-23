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
//	flag:"node-rate-limit"          the command-line flag (required)
//	short:"o"                       an optional one-letter shorthand
//	plan:"export.nodeRateLimit"     the dotted plan.yaml path; absent = not in plan.yaml
//	env:"NODE_RATE_LIMIT"           the variable BYDBCTL_NODE_RATE_LIMIT; absent = no variable
//	default:"0"                     the default, in the same grammar as the flag
//	usage:"..."                     the flag help text
//	parse:"rate-bytes"              the grammar: size, rate-bytes, rate-rows, parallelism,
//	                                selector, format or compress; string, bool, []string and
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
// connection.rateLimit: it is a property of the liaisons, but only an export uses it.
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

// planSchema lists every option struct whose plan tags make up plan.yaml. A plan file is
// checked against all of them, so one file serves every data command.
var planSchema = []any{Connection{}, Export{}}
