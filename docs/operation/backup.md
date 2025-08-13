# Backup Data Using BanyanDB Backup Tool

The backup command-line tool is used to backup BanyanDB snapshots to remote storage. The tool allows both one-time and scheduled backups.

## Overview

The backup tool performs the following operations:

- Connects to a BanyanDB data node via gRPC to request the latest snapshots.
- Determines the local snapshot directories based on the catalog type:
  - **Stream Catalog:** Uses the `stream-root-path`.
  - **Measure Catalog:** Uses the `measure-root-path`.
  - **Property Catalog:** Uses the `property-root-path`.
- Computes a time-based directory name (formatted as daily or hourly).
- Uploads files that are not found in the remote storage to the specified destination.
- Deletes orphaned files in the remote destination that no longer exist locally.
- Optionally schedules periodic backups using cron-style expressions.

## Prerequisites

Before running the backup tool, ensure you have:

- A running BanyanDB data node exposing a gRPC service (by default at `127.0.0.1:17912`).
- Network connectivity to the data node.
- A valid destination URL specified using the `file:///` scheme (or any supported scheme in future releases).
  - Local filesystem: `file:///` scheme
  - AWS S3: S3-compatible URLs with appropriate credentials
  - Azure Blob Storage: Azure URLs with account credentials
  - Google Cloud Storage: GCS URLs with service account
- Necessary access rights for writing to the destination.
- Sufficient permissions to access the snapshots directories for **Stream**, **Measure**, and **Property** catalogs on the data node.

## Command-Line Usage

### One-Time Backup

To perform a one-time backup, run the backup command with the required flags. At a minimum, you must specify the destination URL.

**Example Command:**
```bash
./backup --dest "file:///backups"
```

### Scheduled Backup

To enable periodic backups, provide the `--schedule` flag with a schedule style (e.g., @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>) and set your preferred time style using `--time-style`. The supported schedule expressions based on the tool’s internal map are:

- **Hourly:** Schedules at "5 * * * *" (runs on the 5th minute of every hour).
- **Daily:** Schedules at "5 0 * * *" (runs at 00:05 every day).

**Example Command:**

```bash
./backup --dest "file:///backups" --schedule @daily --time-style daily
```

#### Cloud Storage Examples

Quickly back up to common cloud object stores using the same syntax:

```bash
# AWS S3
# Using credential file
./banyand-backup --dest "s3://my-bucket/backups" \
  --s3-credential-file "/path/to/credentials" \
  --s3-profile "my-profile"

# Using config file
./banyand-backup --dest "s3://my-bucket/backups" \
  --s3-config-file "/path/to/config" \
  --s3-storage-class "GLACIER" \
  --s3-checksum-algorithm "SHA256"

# Google Cloud Storage
./banyand-backup --dest "gs://my-bucket/backups" \
  --gcp-service-account-file "/path/to/service-account.json"

# Azure Blob Storage
# Using account key
./banyand-backup --dest "azure://mycontainer/backups" \
  --azure-account-name "myaccount" \
  --azure-account-key "mykey" \
  --azure-endpoint "https://myaccount.blob.core.windows.net"
  
# Using SAS token
./banyand-backup --dest "azure://mycontainer/backups" \
  --azure-account-name "myaccount" \
  --azure-sas-token "mysastoken"
```

When a schedule is provided, the tool:

- Registers a cron job using an internal scheduler.
- Runs the backup action periodically according to the scheduled expression.
- Waits for termination signals (`SIGINT` or `SIGTERM`) to gracefully shut down.

## Detailed Options

| Flag                | Description                                                                               | Default Value         |
| ------------------- | ----------------------------------------------------------------------------------------- | --------------------- |
| `--grpc-addr`       | gRPC address of the data node.                                                          | `127.0.0.1:17912`     |
| `--enable-tls`      | Enable TLS for the gRPC connection.                                                     | `false`               |
| `--insecure`        | Skip server certificate verification.                                                   | `false`               |
| `--cert`            | Path to the gRPC server certificate.                                                    | _empty_               |
| `--dest`            | Destination URL for backup data (`file:///`, `s3://`, `azure://`, `gs://`)            | _required_            |
| `--stream-root-path`| Root directory for the stream catalog snapshots.                                        | `/tmp`                |
| `--measure-root-path`| Root directory for the measure catalog snapshots.                                      | `/tmp`                |
| `--property-root-path`| Root directory for the property catalog snapshots.                                     | `/tmp`                |
| `--time-style`      | Directory naming style based on time (`daily` or `hourly`)                               | `daily`               |
| `--schedule`        | Schedule expression for periodic backup. Options: `@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`, `@every <duration>` | _empty_               |
| `--logging-level`   | Root logging level (`debug`, `info`, `warn`, `error`)                                   | `info`                |
| `--logging-env`     | Logging environment (`dev` or `prod`)                                                   | `prod`                |
| **AWS S3 specific** |  |  |
| `--s3-profile`      | AWS profile name                                                                        | _empty_               |
| `--s3-config-file`  | Path to the AWS config file                                                            | _empty_               |
| `--s3-credential-file`| Path to the AWS credential file                                                      | _empty_               |
| `--s3-storage-class`| AWS S3 storage class for uploaded objects                                              | _empty_               |
| `--s3-checksum-algorithm`| Checksum algorithm when uploading to S3                                           | _empty_               |
| **Azure Blob specific** |  |  |
| `--azure-account-name`| Azure storage account name                                                           | _empty_               |
| `--azure-account-key`| Azure storage account key                                                            | _empty_               |
| `--azure-sas-token`  | Azure SAS token (alternative to account key)                                         | _empty_               |
| `--azure-endpoint`   | Azure blob service endpoint                                                          | _empty_               |
| **Google Cloud Storage specific** |  |  |
| `--gcp-service-account-file`| Path to the GCP service account JSON file                                      | _empty_               |

This guide should provide you with the necessary steps and information to effectively use the backup tool for your data backup operations.
