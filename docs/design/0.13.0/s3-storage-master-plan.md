<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
-->

# Lifecycle Stage-Based S3 Storage Offloading for BanyanDB

- **Author**: Sruhvx-jpg
- **Status**: Proposed / Review
- **Target Components**: `banyand/backup/lifecycle`, `banyand/internal/storage`, `banyand/metadata`, `pkg/fs/remote/aws`, `banyand/storage/compactor`
- **Issue**: [apache/skywalking#13989](https://github.com/apache/skywalking/issues/13989)

---

## 1. Overview & Context

BanyanDB natively organizes data progression through sequential lifecycle stages (`resource_opts.stages`: e.g., `hot → warm → cold`), historically routing data across heterogeneous machine tiers via `node_selector` (NVMe for ingest-heavy Hot nodes, SATA SSD/HDD for Warm/Cold nodes). 

This proposal upgrades that tiering architecture by introducing **Lifecycle Stage-Based S3 Offloading**:
When an operator designates a lifecycle stage (e.g., `cold`) with S3 storage, the lifecycle agent stops streaming data to dedicated physical storage nodes over gRPC. Instead, it uploads sealed, aged-out local segment files directly into S3, verifies remote integrity, and prunes local disk blocks. Concurrently, the query engine adapts its execution planner to serve that stage's time window directly from S3 using block-aligned HTTP Range-GETs.

### Governing Design Principles
> 1. *"It should be cost efficient"*
> 2. *"It should be highly configurable with best defualt configration"*
> 3. *"should be industry standard mechanism, can als be inspired if 1 and 2 condition is not satisfied."*
> 4. *"Keep warnings as few and only the necessary ones as possible"*

### Key Architectural Decisions (Executive Summary / TL;DR)
1. **Native Metadata Architecture**: All group schemas, retention TTLs, and stage definitions are managed 100% within BanyanDB’s native schema repository (`banyand/metadata`) and node-local configurations with zero external dependencies.
2. **Strict Lifecycle Staged Scope**: Ingestion, active MemTables, and real-time query paths remain untouched on local NVMe. Offloading activates strictly when closed segments transition into an S3-marked stage. Continuous hot-node streaming offloading is explicitly deferred.
3. **Cloud-Agnostic `RemoteStorage` Marker**: Adds a clean sub-message (`provider`, `target`, `identity`, `prefix`) to `LifecycleStage`. Authenticates via standard ambient IAM roles (AWS SDK v2 default provider chain) with zero plaintext keys stored in schemas.
4. **Local Cloud Groups Index (`cloud_groups_index.json`)**: A local on-disk fast-lookup index allows the offloader agent to target only cloud-marked groups directly, skipping non-cloud groups with zero directory traversal overhead.
5. **CRC32C Part Checksums & Anti-PUT Compaction**: Local pre-compaction slashes cloud PUT requests by 90%+. Formal durability requires byte-length match and hardware-accelerated CRC32C checksum verification before local NVMe files are unlinked.
6. **Thanos-Style Crash Recovery Switch**: Checkpoint tracking (`cloud_offload_progress.json`) paired with a 3-mode switch (`auto`, `restart`, `halt-and-alert`) ensures unattended Kubernetes pods never hang on interactive prompts after power cuts or restarts.
7. **Zero-Staging HTTP Range-GET Query Path**: Data nodes never download multi-gigabyte segment archives. Columnar slices (`timestamps.bin`, `fv.bin`) stream on demand via an L1 RAM / L2 NVMe block cache (`FileCache`).
8. **Remote Compactor & Delayed GC**: Out-of-band maintenance worker merges small remote parts, swapping manifests atomically (`<epoch+1>.snp`) with a 1-hour delayed deletion queue protecting active readers from 404s.
9. **Encapsulated Dual Upload Strategy & Shared Driver Reuse**: Reuses existing `pkg/fs/remote/aws` (`s3FS`) and `config.S3Config`, adding `Endpoint` and `UsePathStyle` for MinIO/Ceph compatibility. Encapsulates the dual upload strategy (single PUT for < 16 MB vs chunked multipart for ≥ 16 MB) completely inside `s3FS.Upload()` via AWS SDK v2 `manager.Uploader`. High-level callers (`backup` and `lifecycle`) stay completely decoupled from MPU state and call `fs.Upload(ctx, path, reader)` uniformly. Extends `remote.FS` with targeted sub-interfaces (`StatFS`, `RangeReaderFS`), omitting `MultipartFS` from public caller interfaces.

### Unified Storage Hierarchy & Side Residence Mapping

```
├── Local Active Storage (RAM & NVMe) ─────────────────────────────────┐
│    ├── MemTable (active write buffer in RAM)                         │
│    ├── Active unclosed segments (seg-YYYYMMDD on NVMe)               ├} [STORED LOCALLY]
│    └── cloud_offload_progress.json (ephemeral; pruned after upload)  │
│                                                                      ┘
└── S3 Cloud TSDB (s3://<bucket>/<prefix>/<catalog>/<group>/) ─────────┐
     └── seg-<suffix>/                                                 │
          ├── metadata                                                 │
          ├── sidx/<epoch>.snp                                         ├} [PERMANENT CLOUD]
          └── shard-<shard_id>/                                        │
               ├── manifests/<epoch>.snp                               │
               └── parts/<part_id>/ ───────────────────────────────────┐
                    ├── metadata.json           (part summary)         │
                    ├── meta.bin                (block offsets)        │
                    ├── primary.bin             (primary keys)         │
                    ├── tag.type                (tag types)            ├} [CACHED LOCALLY ON DEMAND]
                    ├── timestamps.bin          [MEASURE/STREAM]       │
                    ├── fv.bin                  [MEASURE values]       │
                    ├── spans.bin               [TRACE spans]          │
                    └── traceID.filter          [TRACE bloom filter]   │
                    └── (Range-GET block slices in FileCache) ──────────┘
```

---

## Subsystem 1: Group Marking & Cloud Storage Schema (`api/proto`, `banyand/metadata`)

### 1. Introduction
The Group Marking & Cloud Storage Schema Subsystem defines how an operator configures and designates a lifecycle stage for cloud offloading within BanyanDB's native metadata repository. It introduces a cloud-agnostic protobuf extension to `LifecycleStage`, maintains a fast-lookup local registry file (`cloud_groups_index.json`) so the agent never scans non-cloud groups, resolves cloud IAM identities securely via the official AWS SDK v2, and provides an optional preflight canary probe to verify cloud permissions before saving schema state.

### 2. How the Design Actually Works

#### Step 1: Protobuf Schema Extension (`api/proto/banyandb/common/v1/common.proto`)
To maintain complete cloud-agnostic neutrality and avoid leaking static credentials into tenant schemas, we introduce the `RemoteStorage` sub-message and embed it as the offload marker inside `LifecycleStage`:

```protobuf
message RemoteStorage {
  // Cloud provider identifier: "s3", "gcs", "azure"
  string provider = 1 [(validate.rules).string = {in: ["s3", "gcs", "azure"]}];

  // Storage bucket or container name (e.g., "banyandb-cold")
  string target = 2 [(validate.rules).string.min_len = 1];

  // Cloud-agnostic IAM identity:
  // - AWS: IAM Role ARN (e.g. "arn:aws:iam::123456789012:role/StorageRole")
  // - GCP: Service Account email
  // - Azure: Managed Identity Client ID
  string identity = 3;

  // Optional root prefix within the bucket (e.g., "prod-cluster")
  string prefix = 4;

  // Optional toggle to trigger the preflight canary probe on creation/update
  bool verify_on_save = 5;
}

message LifecycleStage {
  string name = 1;                     // e.g. "cold"
  uint32 shard_num = 2;
  IntervalRule segment_interval = 3;
  IntervalRule ttl = 4;
  string node_selector = 5;
  bool close = 6;
  uint32 replicas = 7;

  // ---> THE CLOUD OFFLOAD MARKER:
  RemoteStorage remote_storage = 8;
}
```

#### Step 2: Marker Semantics & Stage Routing
The presence of `remote_storage` acts as the explicit binary decision fork for the engine:
- **`remote_storage == nil`**: Standard local physical tier. Data routes across bare-metal data nodes via `node_selector: "type=warm"` (Hot on NVMe, Warm on SATA SSD).
- **`remote_storage != nil`**: Marked cloud stage. The lifecycle agent bypasses data-node gRPC streaming and offloads segment files straight into the designated cloud bucket.

#### Step 3: Local Fast-Lookup Index File (`cloud_groups_index.json`)
To ensure the Cloud Offload Agent never has to traverse the entire catalog or inspect non-cloud group directories, the metadata service writes and maintains a lightweight local index file on the node:
`<root-path>/cloud_groups_index.json`

```json
{
  "updated_at_nanos": 1741764000000000000,
  "groups": [
    {
      "name": "sw_metric",
      "catalog": "CATALOG_MEASURE",
      "cloud_stage": "cold",
      "provider": "s3",
      "target": "banyandb-cold",
      "prefix": "prod-cluster"
    }
  ]
}
```
- **Lifecycle**: Written atomically on every `bydbctl group create/apply/delete` where `remote_storage != nil`.
- **Agent Consumption**: On every offload cycle, the agent reads `cloud_groups_index.json` directly. It immediately targets the exact disk directories of marked groups, skipping 100% of non-cloud groups with zero directory traversal overhead.

#### Step 4: Schema Validation Guardrails
Before accepting a group creation or schema update (`bydbctl group create/apply`), the schema checker (`banyand/metadata/schema/checker.go`) enforces strict policy rules:
1. **Stage 0 (Hot) Ingestion Guard**: The primary ingestion stage (Root `resource_opts`) **MUST NOT** define `remote_storage`. Cloud offloading is legal strictly on transition stages (Stage $\ge 1$).
2. **Sealed Boundary Mandate**: Any stage carrying `remote_storage` must set `close = true`, guaranteeing that unsealed, actively mutating segments are never streamed to object storage.
3. **Immutability of Target Bucket**: Once a stage is active and holds data in S3, updating `target` (bucket) on that stage is blocked to prevent orphaned remote segments.

#### Step 5: Secure Identity Resolution via AWS SDK v2
At runtime, BanyanDB resolves authentication without storing or exposing static secret keys:
- Uses official package `github.com/aws/aws-sdk-go-v2/config` via `config.LoadDefaultConfig`.
- If `remote_storage.identity` is specified, the driver constructs an AssumeRole provider (`stscreds.NewAssumeRoleProvider`) to request short-lived temporary session tokens in memory.
- If `identity` is blank, it automatically defaults to the host node's ambient IAM role / Kubernetes ServiceAccount.

#### Step 6: Optional Preflight Storage Probe (The "Paytm 1-Rupee Test")
When enabled (via `--verify-remote-storage=true` CLI flag or `verify_on_save: true` in schema), the metadata/liaison service runs a lightweight 3-step canary probe before committing the schema to disk:
1. **PUT**: `client.PutObject(ctx, "<prefix>/.banyandb_probe", payload)` $\to$ Verifies write permission (`s3:PutObject`).
2. **GET**: `client.GetObject(ctx, "<prefix>/.banyandb_probe")` $\to$ Verifies read permission (`s3:GetObject`).
3. **DELETE**: `client.DeleteObject(ctx, "<prefix>/.banyandb_probe")` $\to$ Verifies cleanup permission (`s3:DeleteObject`).

If any operation returns an error (e.g. `403 AccessDenied`), the schema update is rejected with an actionable error before any data movement is scheduled.

### 3. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **`cloud_groups_index.json` Corruption or Deletion** | Agent cannot read local index on startup. | Fail-safe: If the file is missing or corrupt, the agent falls back to querying the in-memory `metadata.GroupRegistry()`, rebuilds `cloud_groups_index.json`, and resumes without interruption. |
| **Invalid IAM Role / Missing Permissions** | Offloader or Query engine fails with 403 Access Denied. | Optional preflight probe catches permission gaps at schema creation. If probe is disabled, offloader fails fast during startup and logs exact missing IAM action (`s3:PutObject`, `s3:GetObject`, or `s3:DeleteObject`). |
| **Transient Network Outage During Preflight Probe** | Schema creation could hang or fail intermittently. | Preflight probe has a strict 5-second context timeout with full-jitter retry; if network is unreachable, operator can pass `--skip-verification` or set `verify_on_save=false` to unblock offline deployments. |
| **Operator Attempts Hot Stage Offloading** | Violates staged offload architectural boundary. | Protobuf validator rejects group schemas containing `remote_storage` on root `resource_opts`. |
| **Bucket Non-Existent or Region Mismatch** | S3 API returns 301 Moved Permanently or 404 NoSuchBucket. | AWS SDK v2 auto-resolves bucket region if endpoint is regional, and preflight probe immediately flags non-existent buckets before group creation succeeds. |

### 4. Conclusion
Subsystem 1 decouples physical cloud credentials from tenant schemas while establishing a clean, cloud-agnostic marker (`remote_storage`). By maintaining a fast-lookup local index (`cloud_groups_index.json`), enforcing compile-time protobuf validation, and offering an optional 3-step preflight probe, it guarantees that only valid, reachable, and properly permissioned storage stages can ever be registered in BanyanDB.

---

## Subsystem 2: Agent for Cloud Offloading (`banyand/backup/lifecycle`)

### 1. Introduction
The Agent for Cloud Offloading is the **one-way data migration engine** embedded in BanyanDB's existing lifecycle agent. It identifies closed segments that have aged out of their preceding stage (e.g., `hot` or `warm`) into a cloud-designated stage (e.g., `cold`). It safely packages and streams those immutable parts into S3 object storage, audits remote durability via CRC32C part checksums, and prunes local NVMe blocks without data loss or reader interruption.

### 2. How the Design Actually Works

#### Step 1: Stage Resolution via Fast-Lookup Index
1. The lifecycle agent reads `<root-path>/cloud_groups_index.json` on startup and on every scheduled cycle.
2. It immediately targets only the groups marked with `remote_storage != nil`, bypassing all non-cloud groups with zero directory traversal overhead.
3. For each marked group, it inspects local closed segments (`seg-YYYYMMDD`). When a segment's time boundary exceeds its current stage TTL, the agent branches to the **Agent for Cloud Offloading**.

#### Step 2: Pre-Upload Part Compaction (Default: Enabled)
To prevent S3 PUT bill shock, the agent runs BanyanDB's native LSM merger locally on the sealed segment before hitting the network:
- **Consolidation**: Merges all small, flushed parts in each shard into **one optimized part** per shard.
- **Cost Reduction**: Slashes S3 PUT requests by 85–95% compared to uploading raw micro-flushes.
- **Configurability**: Controlled by `--lifecycle-pre-upload-compact=true` (Default: `true`). If disabled by the operator (`false`), raw parts stream directly to S3 as-is.

#### Step 3: Streamlined Part Streaming via Encapsulated Upload
The agent streams sealed parts to the remote bucket under `s3://<bucket>/<prefix>/<catalog>/<group>/seg-<suffix>/shard-<shard_id>/`:
- **Caller-Agnostic Upload Primitive**: The offloader caller does not manage multipart upload lifecycles, chunk tokens, or branch between single PUT and MPU. It uniformly calls `fs.Upload(ctx, remotePath, reader)`.
- **Encapsulated Dual Strategy in `s3FS.Upload()`**:
  - **Lightweight Components (< 16 MB)**: Handled transparently by the driver via single `PutObject` calls (e.g. `meta.bin`, `bloom-0`).
  - **Heavy Column Blocks (≥ 16 MB)**: Handled transparently by the driver via AWS SDK v2 `manager.Uploader` in 8 MB chunks streamed concurrently with bounded memory buffers (`PartSize: 8MB`, `Concurrency: 4-8`).
- **File-Level Local Checkpointing (`cloud_offload_progress.json`)**: Checkpoints uploaded immutable files locally on NVMe strictly for crash resumption, decoupling the checkpoint from raw in-flight S3 MPU chunk state. Written atomically via temp-file rename (`.tmp` $\to$ `.json`):
  ```json
  {
    "group": "sw_metric",
    "segment": "seg-20260601",
    "shard_id": 0,
    "epoch": 2,
    "first_timestamp": 1741764000000,
    "last_timestamp": 1741850399000,
    "total_records": 1050,
    "status": "UPLOADING",
    "current": "parts/0001/timestamps.bin",
    "upnext": "parts/0001/fv.bin",
    "completed_files": [
      "parts/0001/meta.bin"
    ]
  }
  ```

#### Step 4: Configurable Crash Recovery Switch (`--lifecycle-resume-mode`)
When the agent starts, it checks for an existing `cloud_offload_progress.json`. To prevent unattended background pods from hanging on interactive prompts, recovery behavior is governed by a 3-way switch:
- **`auto` (Default)**: Inspects checkpointed part files. For files marked completed, verifies remote presence via `Stat(ctx, path)` (byte size and checksum match). Uncompleted or interrupted file uploads are restarted cleanly from byte 0 via `fs.Upload()`, while dangling S3 MPU sessions are reaped by the background MPU garbage collector.
- **`restart`**: Aborts active migrations, deletes any staged uncommitted part files under that segment prefix, clears `cloud_offload_progress.json`, and restarts migration of the segment fresh from Part 1.
- **`halt-and-alert`**: Freezes migration on the impacted group, increments the error meter (`pkg/meter`), and alerts the operator for manual verification via `bydbctl`.

#### Step 5: Part Checksum Verification & Safe Local Pruning
Formal durability verification is established through a 3-phase CRC32C checksum lifecycle:
1. **Local Computation**: As the agent reads the local part from NVMe to stream it, it calculates hardware-accelerated `localCRC32C` in memory.
2. **In-Flight Cloud Enforcement**: The agent attaches `localCRC32C` to the S3 upload request (`x-amz-checksum-crc32c`). S3 calculates the checksum on the fly; if network corruption or bit rot occurs in transit, S3 **rejects the upload immediately with a `400 BadDigest` error**.
3. **Post-Upload Formal Verification**: The agent issues a zero-egress `HeadObject` call to S3 and verifies:
   - **Byte-Length Match**: Remote `ContentLength` matches the local part component byte size exactly.
   - **Checksum Match**: Remote `ChecksumCRC32C` matches `localCRC32C` identically.

Only after **100% of the parts** in the shard pass their checksum verification does the agent commit the snapshot:
1. **Pre-Manifest Schema Sync**: The agent verifies that the immutable schema snapshot (`schemas/rev-<revision>.json`) is durable in S3 **before** publishing the manifest, eliminating any crash window with un-decodable data.
2. **Atomic Manifest Publication**: The agent commits `s3://.../shard-<shard_id>/manifests/<epoch>.snp` (embedding `schema_revision: <revision>`) via an atomic conditional write (`If-None-Match: "*"`).
3. The segment is atomically visible to query readers.
4. Local NVMe parts are safely pruned (`os.RemoveAll`).
5. `cloud_offload_progress.json` is deleted from disk.

#### Replica Ownership, Fencing & Pruning Policy

In multi-replica cluster deployments (`replicas > 0`):
1. **Designated Upload Owner**:
   - The primary node assigned by BanyanDB's node registry (`nodes[0]` from `nodeRegistry.Locate(group, name, shardID, 0)`) is the sole upload owner for that shard-segment. Secondary replicas remain idle during offloading.
2. **Atomic Failover Fencing (`If-None-Match: "*"`)**:
   - Shard snapshot manifests use zero-padded monotonic sequence naming (e.g. `manifests/0000000000000002.snp`).
   - The upload owner commits the manifest using an atomic conditional write:
     `s3.PutObject(..., IfNoneMatch: "*")`
   - **Stale Owner Rejection**: If the primary freezes and a promoted replica commits `0000000000000002.snp`, any delayed attempt by the old primary to commit that epoch is **atomically rejected by S3 with `412 Precondition Failed`**. The stale node aborts immediately, preventing split-brain overwrites.
3. **Reader Manifest Discovery (Highest Epoch Wins)**:
   - Query readers list `manifests/` under the shard prefix and deterministically select the lexicographically highest `.snp` file as the authoritative snapshot.
4. **Follower Coverage Verification & Safe Local Pruning**:
   - Secondary replicas do **not** prune local files on bare `Stat` existence.
   - The secondary replica inspects the committed S3 manifest's data boundary (`first_timestamp`, `last_timestamp`, and `total_records`):
     - **Safe to Prune**: If S3 covers the replica's time window and `remote total_records >= local total_records`, the replica safely prunes its local NVMe files.
     - **Divergence Block**: If S3 has fewer records or a narrower time window than the replica holds locally, **local pruning is blocked**, preventing data loss from un-synced replica writes.
   - **Configurable Pruning Toggle**: Adhering to Principle 2, pruning is governed by `--lifecycle-prune-local-replicas=true` (Default: `true` to reclaim NVMe disk). Operators can set this to `false` to retain local replica copies on disk alongside S3 for local fallback.

### 3. Backlog Observability & Backpressure Policy

To prevent un-uploaded segments from exhausting local disk during remote storage stalls, network degradations, or outages, the agent pairs real-time degraded-upload detection with strict multi-tier backpressure policies.

> **Operational Principle (Minimal, High-Signal Alerting)**: Warnings and alarms are kept minimal and strictly restricted to actionable failure thresholds. Transient background retries remain silent to eliminate alert fatigue and log spam; warnings surface only when sustained degradation directly threatens system stability.

#### 1. Defining Slow S3 Uploads & Macro/Micro Observability Warnings

##### Micro-Level Warnings (Socket & Request Layer)
These emit log warnings for possible network or provider degradation at the individual transfer level:
1. **Low-Speed Tripwire (`http_low_speed_limit`)**:
   - If average transfer throughput drops below `2 MB/s` for a sustained window of `30 seconds`, the agent emits a structured log warning.
2. **Rate Throttling Spikes (`503 SlowDown` / `429 Too Many Requests`)**:
   - If >5% of HTTP PUT requests are throttled over a 1-minute window, the remote endpoint is flagged as rate-limited, activating AWS SDK v2 full-jitter exponential backoff and reducing concurrent upload streams.

##### Macro-Level Warning (Pipeline & SLA Layer)
This detects systemic failure where remote offloading falls behind local data generation:
1. **Segment Duration SLA Breach**:
   - Triggered when:
     $$\text{Segment Upload Duration} > \text{Segment Interval}$$
   - *Severity*: **Critical**. If uploading a 1-day segment takes longer than 1 day, the upload drain rate is mathematically slower than the local generation rate, and backlog accumulation is guaranteed. The agent increments `banyandb_lifecycle_upload_sla_breach_total` and fires a critical cluster alert.

#### 2. Upload Backlog Observability
Adhering to Principle 4, observability is kept lean and noise-free. The offloader instruments native meters via `pkg/meter` (Gauges and Counters) alongside structured JSON logs:
- `lifecycle_pending_upload_bytes`: Gauge tracking total byte volume of sealed local segments awaiting S3 upload.
- `lifecycle_oldest_pending_segment_seconds`: Gauge tracking elapsed seconds since the oldest uncommitted segment was sealed.
- `storage_local_disk_free_bytes`: Gauge tracking real-time remaining storage capacity on the local data volume.
- `lifecycle_upload_sla_breach_total`: Counter tracking critical SLA breach events.

#### 3. Multi-Tier Backpressure Rules
Because local segments are strictly retained until full remote manifest commit (`<epoch>.snp`), prolonged S3 outages trigger automatic system-wide self-protection:

1. **Local Compactor (High Watermark: <20% Free Disk)**:
   - When local free disk space drops below `20%`, background local LSM compactions are **immediately paused**.
   - *Rationale*: Merging files temporarily requires up to 2× scratch space ($A + B + C$ alive simultaneously). Pausing compaction eliminates this temporary space spike.
2. **Ingestion (Emergency Watermark: <5% Free Disk)**:
   - When free disk space drops below `5%`, the ingest gateway triggers backpressure (rate-limiting or temporarily rejecting incoming write batches with `RESOURCE_EXHAUSTED`).
   - *Rationale*: Prevents unhandled OS `ENOSPC` (No space left on device) crashes that corrupt write-ahead logs and active MemTables.

### 4. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **Agent Crash Mid-Upload** | Incomplete segment in S3; orphaned MPU chunks. | Governed by `--lifecycle-resume-mode=auto|restart|halt-and-alert`. In `auto` mode, the agent aborts dangling MPU sessions via `AbortMultipartUpload` and resumes only missing parts without re-uploading completed ones. |
| **Crash Before `.snp` Manifest Upload** | Unreferenced parts resting in S3. | **Zero reader corruption**: Readers resolve active parts strictly via `.snp`. A background garbage collection sweep identifies and deletes prefixes lacking a valid `.snp` sentinel. |
| **S3 503 SlowDown / Rate Limiting** | Migration pipeline stalls under S3 request throttling. | Client utilizes AWS SDK v2 full-jitter exponential backoff `(full jitter: randomized sleep between 0 and exponential ceiling to prevent thundering herds)`. |
| **Prolonged Remote Storage Outage / Slowdown** | Uncommitted segments pile up on local disk, risking disk exhaustion. | Strict FIFO offload with low-speed log warning (2 MB/s for 30s); pauses local compaction at <20% disk to eliminate 2× scratch spike; throttles ingestion at <5% disk to prevent node panics. |
| **Zombie Primary Resumes After Failover** | Stale primary wakes up after promotion and attempts to commit. | S3 atomically rejects the upload with `412 Precondition Failed` via `If-None-Match: "*"`. Stale node detects conflict, aborts, and purges local scratch files with zero corruption to the active snapshot. Covered by dedicated failure unit/E2E test. |
| **Local Disk Exhaustion During Pre-Compaction** | Machine runs out of scratch space before uploading. | Pre-compaction streams with bounded memory (128–256 MB buffer ceiling) or writes to a dedicated scratch volume (`--lifecycle-scratch-dir`) monitored by low-watermark safety checks. |
| **Schema Deletion During Migration (Orphan Rows)** | Schema removed while segment is queued for S3 upload. | Native orphan handling routes deleted measure/stream rows to the local orphan archive (`--migration-orphan-policy=archive`), allowing remaining valid columnar parts to upload cleanly. |

### 5. Conclusion
The Agent for Cloud Offloading provides a linear, crash-resilient ingress path into object storage. Governed by strict design principles, it couples pre-upload compaction with a 3-way configurable recovery switch (`auto`, `restart`, `halt-and-alert`), atomic `.snp` commits, and hardware-accelerated CRC32C part checksum verification, ensuring local NVMe blocks are only reclaimed after remote cloud durability is 100% verified.

---

## Subsystem 3: Crash Recovery & Orphan Session Manager (`banyand/backup/lifecycle`)

### 1. Introduction
The Crash Recovery & Orphan Session Manager is the **fail-safe state reconciler** for the Cloud Offloader. When a data node or agent process crashes, restarts, or loses network connectivity mid-transfer, this engine audits residual state from `cloud_offload_progress.json`, cleans up dangling S3 Multipart Uploads (MPUs), and executes a configurable recovery policy (`auto`, `restart`, `halt-and-alert`) without hanging unattended background services.

### 2. Industry Standard Provenance
This subsystem is modeled directly on proven, battle-tested cloud storage patterns in real-world distributed databases:
1. **Thanos & Prometheus TSDB (`thanos.shipper.json`)**: Checkpoint-driven upload resumption where the shipper verifies existing S3 blocks via metadata calls and resumes without re-uploading finished blocks.
2. **Apache Doris & ClickHouse (Rowset S3 Publisher)**: 24-hour MPU lifecycle cleanup where background sweeps abort abandoned multipart sessions older than 24 hours to prevent cloud billing leaks.
3. **Snowflake & Databricks AutoLoader**: The 3-mode recovery switch (`auto`, `restart`, `halt-and-alert`) decoupling automatic idempotent retry from clean-slate restarts and circuit-breaking alerts.

### 3. How the Design Actually Works

#### Step 1: Startup Interruption Audit
Before initiating any new segment migration, the lifecycle agent inspects its local catalog directory for residual checkpoint state:
`<root-path>/cloud_offload_progress.json`

- **If missing**: Clean startup. The agent proceeds to normal scheduled offloading.
- **If present**: An interrupted session is detected. The agent immediately halts new migrations for that group and routes execution into the recovery pipeline.

#### Step 2: The Recovery Mode Switch (`--lifecycle-resume-mode`)
To prevent headless Kubernetes pods from freezing on interactive prompts, the operator governs recovery behavior via an explicit CLI flag:

```bash
lifecycle \
  --lifecycle-resume-mode=auto \        # Options: auto | restart | halt-and-alert (Default: auto)
  --lifecycle-orphan-mpu-ttl=24h        # Max age before aborting dangling S3 MPU sessions
```

##### Mode A: `auto` (Default & Production Baseline)
1. **File-Level Resume & Remote Durability Verification**: For all part files flagged as completed in `cloud_offload_progress.json`, issues `Stat` calls (`s3.HeadObject`) to verify they exist in S3 with matching byte sizes. Any file that failed mid-upload is cleanly re-uploaded via `fs.Upload()`.
2. **Seamless Resumption**: Skips confirmed part files and resumes uploading only the missing files.
3. **Commit & Clean**: Swaps `<epoch>.snp`, verifies 100% segment integrity, unlinks local NVMe files, and deletes `cloud_offload_progress.json`.

##### Mode B: `restart` (Clean Slate)
1. **Remote Purge**: Calls `s3.AbortMultipartUpload` on all active sessions and deletes any staged part files under that segment prefix in S3.
2. **Local Reset**: Deletes `cloud_offload_progress.json`.
3. **Fresh Execution**: Re-queues the sealed segment for clean, full pre-compaction and upload from Part 1.

##### Mode C: `halt-and-alert` (Strict Manual Inspection)
1. **Circuit Breaker**: Freezes offloading for the affected group to prevent race conditions or cascading failures. Other non-impacted groups continue operating normally.
2. **Telemetry & Observability**: Increments native error meter `lifecycle_interrupted_session_total` (`pkg/meter`) and logs a structured error with exact resume tokens.
3. **Manual Operator Release**: Waits for an operator to resolve the session manually via `bydbctl` or by clearing the checkpoint file:
   ```bash
   bydbctl lifecycle resume --group=sw_metric --segment=seg-20260601 --mode=auto
   ```

#### Step 3: Global S3 Multipart Upload (MPU) Garbage Collector
S3 chunks from incomplete Multipart Uploads consume billable storage indefinitely unless explicitly committed or aborted.
- On agent startup and once every 24 hours, the manager runs an out-of-band sweep:
  1. Issues `s3.ListMultipartUploads` under the cluster's bucket prefix.
  2. Identifies any orphaned upload sessions initiated by BanyanDB older than `--lifecycle-orphan-mpu-ttl=24h`.
  3. Executes `s3.AbortMultipartUpload`, purging orphaned cloud chunks to zero byte waste.

### 4. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **Crash Mid-Write of `cloud_offload_progress.json`** | Local checkpoint file is corrupted with half-written JSON. | **Atomic write with rename**: Checkpoint writes to `cloud_offload_progress.json.tmp` and executes an atomic POSIX rename (`os.Rename`). If corrupted, the manager falls back to `restart` mode for that segment. |
| **S3 Returns `NoSuchUpload` on Abort** | Agent attempts to abort an MPU that already expired on S3. | Idempotent handling: `NoSuchUpload` errors are treated as successful deletions; execution proceeds without failing. |
| **Repeated Crash Loop on Same Part** | Persistent poisoned data block causes endless crashes. | Manager tracks crash retry attempts inside `cloud_offload_progress.json` (`retry_count`). After 3 consecutive crash-recovery cycles on the same segment, the manager automatically escalates to `halt-and-alert` to break the crash loop. |
| **Network Loss During Recovery Handshake** | Cannot reach S3 to audit existing completed parts. | Applies AWS SDK v2 full-jitter exponential backoff. If connectivity remains severed past timeout, agent backs off and re-attempts next cycle. |

### 5. Conclusion
Subsystem 3 ensures BanyanDB remains truly hands-off and production-grade. By combining an industry-standard 3-way configurable recovery switch (`auto`, `restart`, `halt-and-alert`) modeled on Thanos, Doris, and Databricks with atomic local checkpoints and background S3 MPU garbage collection, it guarantees zero unattended pod freezes, zero cloud billing leaks from dangling chunks, and deterministic recovery after infrastructure crashes.

---

## Subsystem 4: Data Node Query Engine & Two-Tier Caching Layer (`banyand/internal/storage`)

### 1. Introduction
The Data Node Query Engine serves time-series telemetry queries against cloud-backed stages directly from S3 without full-segment pre-staging, ensuring sub-millisecond response latency for warm/hot queries and minimal network egress costs for cold data.

### 2. How the Design Actually Works

#### Step 1: Zero-Staging HTTP Range-GET Reader
Data nodes never download multi-gigabyte segment archives to answer queries:
- Readers parse the remote part's `meta.bin` descriptor to identify exact block offsets.
- Readers issue byte-aligned HTTP requests (`s3.GetObjectInput{Range: "bytes=1048576-2097151"}`) to stream only the specific columnar slices (`timestamps.bin`, `fv.bin`) required by the query projection and time filters.
- Eliminates local disk staging overhead and bounds in-flight memory to active query block sizes.

#### Step 2: Native Filter Query Pruning (Network Bandwidth Shield)
For specific ID lookups (e.g. `trace_id = "abcd-1234"` or entity ID queries):
- The query engine fetches the small native filter file first (e.g. `traceID.filter` for traces, or secondary index filters).
- If the filter test returns negative, the part is completely bypassed over the wire.
- Prevents downloading hundreds of megabytes of columnar payloads for parts that do not contain the target series.

#### Bounded In-Memory & Local Disk Caching

All cache pools provide production-ready defaults and are fully configurable via node CLI flags and configuration files, allowing operators to easily scale budgets up or down based on host resources:

##### 1. Partitioned In-Memory Cache (RAM)
To prevent heavy Bloom filters from evicting critical metadata, memory is split into two isolated LRU pools:
- **Metadata Pool (`meta.bin`, `.snp`, `metadata.json`)**: Default `128MB` (`--s3-cache-meta-max-memory=128MB`). Eviction: Isolated LRU.
- **Filter Pool (`traceID.filter`, secondary index filters)**: Default `512MB` (`--s3-cache-bloom-max-memory=512MB`). Eviction: Isolated LRU. Filters never evict metadata.
- *Default Total RAM Footprint*: ~640 MB. Fully configurable.

##### 2. Local Disk Cache (NVMe)
- **Data Block Cache**: Default `10GB` (`--s3-cache-disk-max-size=10GB`, dir: `--s3-cache-dir=/data/s3-cache`). Caches 1 MB–4 MB columnar slices on demand. Eviction: Block-level LRU. 100% ephemeral. Fully configurable.

##### 3. Read Concurrency Limit
- `--s3-max-concurrent-readers=32` (Default: 32). Concurrency semaphore preventing heavy queries from exhausting network sockets or CPU. Fully configurable.

#### Step 4: Cache Invalidation on Remote Compactions (Zero RPC Overhead)
When the background cloud compactor consolidates parts:
- **Versioned Block Keys**: Cache keys are strictly namespaced by immutable part ID:
  `<catalog>/<group>/<seg>/<shard>/<part_id>/<file>/<offset>`
- When remote compaction replaces `part-1` and `part-2` with `part-3`, new queries reading `<epoch+1>.snp` automatically target and cache blocks under `part-3`.
- Blocks belonging to superseded parts stop receiving query hits and are naturally evicted by the LRU queue as new data arrives, requiring zero distributed cache invalidation RPCs.

### 3. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **Local Cache Block Corruption** | Corrupt block causes decompression/checksum failure. | Evicts the corrupted block from the cache index immediately; transparently falls back to an S3 Range-GET to stream a clean copy and re-populate the block cache. |
| **S3 503 SlowDown / Throttling During Burst Query** | Query latency spikes due to S3 rate limiting. | Query reader leverages HTTP/2 pooled transports and exponential backoff with full jitter (AWS SDK v2 standard) across pre-warmed connection pools. |
| **High Cache Churn / Thrashing** | Cache capacity exhausted by massive ad-hoc scans. | Block cache isolates index metadata (L1) from data blocks (L2). Large scans pass through a transient streaming buffer rather than blowing out the entire L2 NVMe cache. |
| **Concurrent Remote Compaction Swap** | Reader accesses a part currently being deleted by GC. | Readers pin the active `<epoch>.snp` snapshot for the duration of the query. Compactor delayed GC queue guarantees superseded parts are preserved for a safety window (default: 1 hour). |

### 4. Conclusion
Subsystem 4 delivers cost-efficient, low-latency historical queries against S3. By pairing zero-staging HTTP Range-GETs and Bloom filter pruning with a rebuildable two-tier LRU cache (`FileCache`), it eliminates full-segment downloads while maintaining sub-millisecond query speed on cached blocks.

---

## Subsystem 5: Remote S3 Compactor (`banyand/storage/compactor`)

### 1. Introduction
The Remote S3 Compactor is the **out-of-band maintenance engine** responsible for consolidating small remote parts within each S3 shard. It prevents HTTP Range-GET latency explosions and S3 list request amplification by maintaining optimal part sizes and managing delayed garbage collection.

### 2. How the Design Actually Works

#### Step 1: Data-Blind Merge Planning
The compactor plans merges without downloading raw columnar data:
- Downloads only lightweight part `metadata.json` summary files (<1 KB) to memory to inspect timestamps, row counts, and block boundaries. (Binary block descriptors `meta.bin` are accessed only during merge execution).
- Evaluates BanyanDB's native size-tiered merge ratio math (`mergePolicy.getPartsToMerge`) to select candidates for consolidation.

#### Step 2: K-Way Merge Sort via Local Scratch Buffer
Once candidate parts are selected:
- The compactor streams selected parts using bounded RAM buffers (4 MB–8 MB) or a dedicated local scratch volume (`--compactor-scratch-dir=/tmp/banyandb-scratch`).
- Executes a K-way sorted merge, grouping columnar data into a new consolidated part (`part-<new_id>`).
- Regenerates native filters: serializes `traceID.filter` for Trace parts, and merges `primary.bin` alongside columnar files for Measure and Stream parts.

#### Step 3: Compaction Priority Scoring & Scheduling
The compactor schedules work dynamically based on fragmentation severity:
$$\text{Priority Score} = \frac{\text{Part Count}}{\text{Compaction Level} + 1}$$
- Shards uploaded with higher part fragmentation carry a higher priority score and are placed into the immediate consolidation queue.
- Shards pre-compacted by the offloader bypass cumulative merges and wait for macro-level segment consolidation.

#### Step 4: Atomic Manifest Swap & Concurrency Control
- **Concurrency Control**: Shard compaction is allocated deterministically per compactor worker instance (or single-compactor deployment).
- **Atomic Swap**: The compactor uploads all new part files first. Once fully durable, it commits the merged output to S3 by uploading `<epoch+1>.snp`. Readers instantly switch to the new manifest on their next query.

#### Step 5: Delayed Garbage Collection Queue
To prevent active queries reading `<epoch>.snp` from encountering 404 errors:
- Superseded source parts are not deleted immediately.
- The compactor enqueues superseded part keys into a delayed deletion queue (default: 1 hour).
- Once the safety window elapses, a background worker issues batch `DeleteObjects` to reclaim remote cloud space.

### 3. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **Compactor Crash Mid-Merge** | Staged merged part in S3; local scratch files. | **Zero reader corruption**: Active `<epoch>.snp` still references original source parts. On restart, compactor purges local scratch files and GC sweeps unreferenced remote parts. |
| **Scratch Disk Space Exhaustion** | Merge worker runs out of local scratch capacity. | Scratch volume is monitored by low-watermark safety checks. If disk utilization crosses 80%, compaction throttles or aborts cleanly without data loss. |
| **Dangling Old Parts After Manifest Swap** | Superseded parts remain in S3 indefinitely if GC crashes. | Background GC periodically scans S3 prefixes for part IDs not referenced in the latest active `<epoch>.snp` snapshot and sweeps them after the safety TTL. |

### 4. Conclusion
Subsystem 5 maintains long-term S3 query efficiency without external consensus overhead. Through data-blind planning, atomic manifest swapping, and delayed garbage collection, it bounds HTTP Range-GET amplification while guaranteeing complete reader isolation.

---

## Subsystem 6: Pluggable Remote Storage Driver (`pkg/fs/remote/aws`)

### 1. Introduction
The Pluggable Remote Storage Driver provides a resilient, unified object storage client layer built on the official AWS SDK for Go v2. It abstracts cloud object operations across AWS S3, MinIO, Ceph, and Google Cloud Storage (S3-compatible mode). Rather than creating a separate client or exposing low-level multipart primitives to higher layers, it encapsulates the dual upload strategy within `s3FS.Upload()` and enhances the existing driver shared across both Backup and Lifecycle Offloading.

### 2. How the Design Actually Works

#### Step 1: Mapping to Existing Implementation & Config Extensions
The driver directly builds on BanyanDB's existing `type s3FS struct` in `pkg/fs/remote/aws/s3.go` and `config.S3Config`:
- **Existing Client Reuse**: Retains the AWS SDK v2 client initialization (`s3.NewFromConfig`), credentials resolution, bucket mapping, and storage class configuration.
- **Operational Configuration Extensions**: Extends `config.S3Config` with missing enterprise fields:
  ```go
  type S3Config struct {
      // ... existing fields ...
      Endpoint     string `json:"endpoint,omitempty"`
      UsePathStyle bool   `json:"use_path_style,omitempty"`
  }
  ```
  Exposed via CLI flags `--s3-endpoint` and `--s3-use-path-style=true`. Enables seamless compatibility with MinIO, Ceph Object Gateway, and Cloudflare R2 without custom forks.

#### Step 2: Encapsulated Dual-Strategy Uploader (`s3FS.Upload`)
Instead of exposing granular MPU primitives (`CreateMultipart`, `UploadPart`, etc.) to callers, `s3FS.Upload()` internally encapsulates the dual upload strategy using the official AWS SDK v2 manager (`github.com/aws/aws-sdk-go-v2/feature/s3/manager`):
- **Unified Caller Contract**: Callers (`banyand/backup` and `banyand/backup/lifecycle`) strictly invoke `fs.Upload(ctx, remotePath, reader)`. Callers do not manage upload IDs, part numbers, or chunk buffers.
- **Driver-Internal Decision Engine**:
  ```go
  func (s *s3FS) Upload(ctx context.Context, path string, data io.Reader) error {
      key := s.getFullPath(path)
      uploader := manager.NewUploader(s.client, func(u *manager.Uploader) {
          u.PartSize = 8 * 1024 * 1024 // 8 MB chunks
          u.Concurrency = 4            // Bounded concurrency (prevents memory exhaustion)
      })

      _, err := uploader.Upload(ctx, &s3.PutObjectInput{
          Bucket:            aws.String(s.bucket),
          Key:               aws.String(key),
          Body:              data,
          ChecksumAlgorithm: s.checksumAlgorithm,
          StorageClass:      s.storageClass,
      })
      return err
  }
  ```
- **Transparent Execution**:
  - Files < 16 MB: The uploader automatically executes a single, fast `PutObject` call in memory.
  - Files ≥ 16 MB: The uploader automatically initiates a Multipart Upload, streaming 8 MB chunks concurrently with bounded memory buffers (`PartSize: 8MB`, `Concurrency: 4`), completing or aborting the upload on error.

#### Step 3: Capability Sub-Interface Extensions on `remote.FS`
To keep `remote.FS` minimal while supporting verification and query Range-GETs, targeted capability sub-interfaces are introduced:
```go
// StatFS provides metadata inspection for pre-pruning integrity audits.
type StatFS interface {
    remote.FS
    Stat(ctx context.Context, path string) (ObjectInfo, error)
}

// RangeReaderFS provides byte-slice streaming for zero-staging query execution.
type RangeReaderFS interface {
    remote.FS
    DownloadRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
}
```
- **`StatFS` Implementation**: Implemented in `s3FS` via `s3.HeadObjectInput`. Enables the lifecycle offloader to verify remote byte size against local NVMe files before executing safe pruning (`os.RemoveAll`).
- **`RangeReaderFS` Implementation**: Implemented in `s3FS` via `s3.GetObjectInput{Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))}`. Enables the query engine to stream columnar blocks without full segment staging.
- **`MultipartFS` Omission**: Explicitly omitted from caller interfaces because MPU lifecycle management is fully encapsulated within `s3FS.Upload()`.

#### Step 4: Backup Subsystem Integration & 5 GiB Cap Elimination
BanyanDB's existing backup command (`banyand/backup/backup.go`) calls `fs.Upload()` to archive segment and snapshot data. By upgrading `s3FS.Upload()` with the encapsulated uploader:
- **5 GiB Hard Limit Eliminated**: Fixes the S3 `EntityTooLarge` crash when backing up large shard SST files > 5 GiB.
- **Zero Caller Refactoring**: `banyand/backup` code requires zero changes to support multipart uploads; it transparently inherits MPU capabilities through `fs.Upload()`.
- **MinIO/Ceph Backups Unlocked**: Backups can now target custom S3-compatible endpoints via the new `--s3-endpoint` and `--s3-use-path-style` configuration.
- **Completeness Verification**: Backup verification can leverage `StatFS` to ensure uploaded archive files are intact in S3 before marking a backup run complete.

#### Step 5: HTTP/2 Transport & Connection Pooling
To eliminate handshake latency during high-frequency Range-GET queries:
- Configures pre-warmed HTTP/2 persistent connection pools.
- Uses persistent TCP keep-alive probes to maintain warm connections to S3 endpoints, slashing round-trip TLS negotiation overhead.

#### Step 6: Full-Jitter Exponential Backoff
To handle S3 `503 SlowDown` throttling during high-throughput migration bursts:
$$\text{Sleep} = \text{random}(0, \min(\text{MaxBackoff}, \text{Base} \times 2^{\text{attempt}}))$$
- Full jitter prevents synchronized thundering herd retries across data nodes.

#### Progressive Backend Compatibility

To support private clouds and hybrid environments alongside AWS S3, the driver provides standard compatibility flags (`--s3-endpoint` and `--s3-use-path-style=true`).

Backend-specific compatibility and quirks (AWS S3, MinIO, Ceph, Cloudflare R2) mature iteratively during subsystem implementation, with MinIO serving as the baseline target in CI.

### 3. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **S3 503 SlowDown / Rate Limiting** | Spike in request latency during heavy traffic. | Handled automatically via SDK v2 standard retry policy with full jitter and configurable max retry limits (`--s3-max-retries=5`). |
| **Stale TCP Connection Resets** | Premature connection close by remote load balancer. | Connection pool sets aggressive idle connection timeouts (`IdleConnTimeout: 90s`) and active keep-alive probes. |

### 4. Conclusion
Subsystem 6 establishes a resilient, production-hardened network layer for all cloud operations in BanyanDB, ensuring maximum throughput, zero connection leaks, and seamless compatibility across AWS S3, MinIO, and Ceph.

---

## Subsystem 7: Remote Schema Mirroring & Disaster Recovery (`banyand/metadata`, `bydbctl`)

### 1. Introduction
The Remote Schema Mirroring & Disaster Recovery subsystem guarantees that S3 remains 100% self-describing and recoverable if an entire cluster (all data nodes and native metadata stores) is lost. By mirroring group and resource schemas directly to S3 alongside telemetry segments, a brand-new, empty BanyanDB cluster can reconstruct its native `banyand/metadata` catalog from S3 and immediately resume serving historical queries.

### 2. How the Design Actually Works

#### 1. Immutable Remote Schema Snapshots (`schemas/rev-<revision>.json`)
The source of truth in disaster scenarios is the set of immutable versioned schemas under:
`s3://<bucket>/<prefix>/<catalog>/<group>/schemas/rev-<revision>.json`

- **Versioned Immutability**: Schemas are stored as write-once revision files. Delayed old writers cannot overwrite newer schemas because each revision carries a unique monotonic revision number.
- **Manifest Binding**: Each shard snapshot manifest (`.snp`) explicitly embeds its required `"schema_revision": <revision>`, ensuring readers always decode parts using the exact schema revision that produced them.
- **Latest Pointer (CAS with `If-Match`)**: An authoritative `schemas/latest.json` pointer references the newest active revision. Initial creation uses `If-None-Match: "*"`. Subsequent revision updates use **`If-Match: <current_etag>`** (Compare-And-Swap), checking that the incoming revision strictly increases (`revision_new > revision_current`) and re-reading/reconciling on conflict, preventing stale overwrites.
- **When it is written**:
  1. **At Schema Creation/Update**: Triggered whenever an operator applies changes via `bydbctl`.
  2. **Pre-Manifest Sync**: The offload agent confirms `schemas/rev-<revision>.json` is durable in S3 before committing each segment's `.snp` manifest.
- **Payload**: Contains the complete native Protobuf dependency set:
  - `commonv1.Group`: Base namespaces, segment interval, TTL, and lifecycle stages.
  - `databasev1.IndexRule`: Secondary and inverted index configurations.
  - `databasev1.Measure` / `databasev1.Stream` / `databasev1.Trace`: Columnar tag families, fields, and codecs across all three catalogs.
  - `databasev1.IndexRuleBinding`: Ties index rules to target subjects.

#### 2. The Disaster Recovery Workflow (Total Loss Scenario)
When local infrastructure is destroyed and a fresh cluster is booted with a blank local metadata store:

1. **Bootstrap Command**:
   The operator points `bydbctl` to the surviving S3 bucket:
   ```bash
   bydbctl restore from-remote \
     --provider=s3 \
     --target=<bucket> \
     --prefix=<prefix>
   ```
2. **Dependency-Ordered Catalog Restoration**:
   `bydbctl` imports metadata into the new native `banyand/metadata` registry in strict dependency order:
   - **Step 1: Groups (`commonv1.Group`)** $\to$ Registers namespaces first.
   - **Step 2: Index Rules (`databasev1.IndexRule`)** $\to$ Defines index specs before bindings.
   - **Step 3: Resources (`Measure`, `Stream`, `Trace`)** $\to$ Registers all data schemas.
   - **Step 4: Index Rule Bindings (`databasev1.IndexRuleBinding`)** $\to$ Binds rules to subjects.
3. **Remote Segment Discovery**:
   - The query engine scans shard snapshot manifests (`manifests/<epoch>.snp`) in S3.
   - Historical queries across measures, streams, and traces become operational immediately via HTTP Range-GETs without downloading or re-ingesting raw data.

### 3. Theoretical Failures & Edge Cases

| Failure Mode / Edge Case | System Impact | Mitigation & Recovery |
| :--- | :--- | :--- |
| **Schema Update During Network Outage** | Schema saved locally in `banyand/metadata` but failed to mirror to S3. | The offloader checks `schemas/rev-<revision>.json` before segment manifest publication, uploading the immutable revision file if missing. |
| **Concurrent Schema Modifications** | Race condition between multiple operators. | Immutable revision files (`schemas/rev-<revision>.json`) are write-once (`If-None-Match: "*"`). The `schemas/latest.json` pointer advances via CAS (`If-Match: <etag>`), rejecting stale lower revisions and re-reading on conflict. |
| **Corrupted Schema in S3** | JSON payload fails protobuf deserialization during recovery. | Writes to S3 enforce CRC32C checksums. Revisions are immutable; if `latest.json` is corrupted, recovery falls back to the highest valid `schemas/rev-<revision>.json`. |

### 4. Conclusion
Subsystem 7 eliminates external metadata dependencies during catastrophic failures. By persisting self-describing immutable schema snapshots alongside segment data in S3 and providing a single-command restore flow via `bydbctl`, BanyanDB guarantees zero data or metadata loss **for all offloaded lifecycle stages** after total hardware destruction (active hot-tier data remaining on local nodes continues to be protected by native local replica policies).

---

## Existing Core Behavior vs. New S3 Work Matrix

| Subsystem / Area | **Existing BanyanDB Core (Untouched)** | **New S3 Cloud Work (To Implement)** |
| :--- | :--- | :--- |
| **Storage & LSM Engine** | Native LSM tree, `mergePolicy.getPartsToMerge` (size ratio math), local `.snp` snapshot parser, SST columnar format (`.bin`). | S3 HTTP Range-GET reader, S3 two-tier local block cache (`FileCache`), remote multipart streaming. |
| **Trace Subsystem** | `Trace Fragment Guard` execution logic (`docs/.../trace-fragment-sampling-guard.md`), span grouping by `trace_id`. | S3 Bloom filter upload persistence, remote Bloom filter fetching for unselected candidate parts, merged output Bloom serialization. |
| **Lifecycle & Offloading** | In-memory MemTable flusher, local segment timestamp boundary rotation, TTL calculation. | S3 Lifecycle Agent offloader (`banyand/backup/lifecycle`), S3 CRC32C checksum auditor, local NVMe pruning gate (`StatFS`), file-level `cloud_offload_progress.json` resume. |
| **Compactor** | Local single-shard merger, sorted K-way iterator (`mergeParts` $\to$ `mergeBlocks`). | Remote S3 Compactor daemon, atomic S3 `.snp` manifest swapping, delayed GC queue. |
| **Cluster Metadata** | Native metadata schema repository (`banyand/metadata`), node discovery, group catalog tracking. | Protobuf extensions (`RemoteStorage` sub-message on `LifecycleStage`), `cloud_groups_index.json` local index. |
| **Remote Driver (`pkg/fs/remote/aws`)** | Basic `s3FS` with single `PutObject`, AWS default config. | Encapsulated dual-strategy `s3FS.Upload()` (`manager.Uploader`), `StatFS` (`s3.HeadObject`), `RangeReaderFS` (`s3.GetObjectInput{Range}`), `Endpoint` & `UsePathStyle` config for MinIO/Ceph. |
| **Backup Subsystem (`banyand/backup`)** | Snapshot exporter using `remote.FS.Upload()`, 5 GiB S3 cap. | Transparently inherits encapsulated multipart upload (zero 5 GiB cap), custom S3 endpoints, and `Stat()` archive verification. |
| **Disaster Recovery (`banyand/metadata`, `bydbctl`)** | Manual cluster re-creation, node-local metadata. | Automated remote schema mirroring (`schemas/rev-<rev>.json` + CAS `latest.json`), `bydbctl restore from-remote` zero-state bootstrap from S3. |

---

## Failure Recovery Matrix

| Failure Mode | Impact | Recovery Action |
| :--- | :--- | :--- |
| **Lifecycle Agent crash mid-upload** | Orphaned chunks in S3; incomplete local state. | Agent reads local `cloud_offload_progress.json`, aborts abandoned MPU sessions, and resumes missing parts based on `--lifecycle-resume-mode`. |
| **Crash before uploading `.snp`** | Unreferenced parts in S3. | Fail-safe: Data nodes never see uncommitted parts because active `.snp` was not swapped. Background GC sweeps orphaned prefixes. |
| **Compactor crash mid-merge** | Staged merged part in S3; local scratch files. | Zero corruption: Active `.snp` still references old parts. New compactor clears scratch disk and deletes unreferenced parts. |
| **S3 503 SlowDown / Throttling** | Request latency spike. | S3 client applies exponential backoff with full jitter (AWS SDK v2 standard) with pooled HTTP/2 transports. |
| **Local Cache Corruption** | Cache read checksum failure. | Evict corrupt block from cache index; transparently fall back to S3 Range-GET and re-populate block cache. |
| **Total Loss (Nodes + Metadata Dead)** | Cluster destroyed; only S3 storage survives. | Run `bydbctl restore from-remote` to re-import immutable schemas (`schemas/rev-<rev>.json`) into fresh native `banyand/metadata` and query remote segments immediately. |

---

## E2E Test Suite (In-Progress)

A dedicated automated E2E test suite running against MinIO is developed simultaneously alongside subsystem implementation to validate failure boundaries, tune thresholds, and confirm production defaults:

1. **Workload Isolation**: Verify cold historical S3 scans do not degrade concurrent hot writes and dashboard query p99 latency by $>10\%$.
2. **Prolonged Outage & Disk Pressure**: Simulate sustained S3 unavailability to verify multi-tier backpressure (compaction pause, ingestion throttling) prevents local disk exhaustion.
3. **Replica Ownership & Failover**: Validate single-writer upload via `NodeRegistry.Locate()` and epoch fencing during replica failovers.
4. **Disaster Recovery**: Verify end-to-end cluster restoration and queryability from S3 after total loss of local data nodes and the native metadata registry, asserting query correctness across Measure, Stream, and Trace catalogs with active IndexRuleBindings.
5. **Cache Exhaustion**: Saturate In-Memory and Local Disk caches to verify isolated LRU eviction under memory pressure with zero OOM panics.

---

## Master Plan Summary

This proposal establishes S3 as an immutable, pluggable storage tier for BanyanDB, operating strictly on defined lifecycle stages without external consensus daemons. All metadata and stage definitions remain 100% within BanyanDB's native schema repository (`banyand/metadata`) and node-local configurations, ensuring zero credential leaks, seamless query Range-GET execution, and resilient crash recovery across all cloud operations.

All default values in this proposal are experimental and subject to change in the future based on E2E tests and community feedback.
