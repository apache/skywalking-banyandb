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
- Necessary access rights for reading snapshot data and writing to the destination.

## Command-Line Usage

### One-Time Backup

To perform a one-time backup, run the backup command with the required flags. At a minimum, you must specify the destination URL.

**Example Command:**
```bash
./bydbbackup backup --dest "file:///backups"
```

### Scheduled Backup

To enable periodic backups, provide the `--schedule` flag with a schedule style (e.g., `hourly` or `daily`) and set your preferred time style using `--time-style`. The supported schedule expressions based on the tool’s internal map are:

- **Hourly:** Schedules at "5 * * * *" (runs on the 5th minute of every hour).
- **Daily:** Schedules at "5 0 * * *" (runs at 00:05 every day).

**Example Command:**

```bash
./bydbbackup backup --dest "file:///backups" --schedule daily --time-style daily
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
| `--stream-root-path`| Root directory for the stream catalog snapshots.                                        | `/tmp`                |
| `--measure-root-path`| Root directory for the measure catalog snapshots.                                      | `/tmp`                |
| `--property-root-path`| Root directory for the property catalog snapshots.                                     | `/tmp`                |
| `--dest`            | Destination URL for backup data. (e.g., `file:///backups`)                                | _required_            |
| `--time-style`      | Directory naming style based on time. Supports `daily` or `hourly`.                       | `daily`               |
| `--schedule`        | Schedule style for periodic backup. If not set, backup is performed once. Options: `hourly` or `daily`. | _empty_               |

This guide should provide you with the necessary steps and information to effectively use the backup tool for your data backup operations.
