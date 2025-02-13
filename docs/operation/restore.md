# Restore BanyanDB Data Using the Restore Tool

This document explains how to use the backup and restore command line tools for BanyanDB.

## Overview

- **Backup Tool:**  
  Backs up snapshot data from the BanyanDB data node to a remote storage destination. Files are organized under a time-based directory (e.g., daily or hourly) computed via a configurable time style.

- **Restore Tool:**  
  Reads the *timedir* files from local catalog directories and uses the timestamp within to determine which remote backup snapshot should be applied. Once restoration is successful, it removes all *timedir* files to avoid repeated restore operations, especially during unexpected scenarios.

- **Timedir Utility:**  
  Provides commands to create, list, read, and delete *time-dir* marker files for each catalog (e.g., _stream_, _measure_, and _property_).

## Restore Workflow

In an on-prem deployment, the BanyanDB data node runs on a dedicated server (or virtual machine), and backup/restore operations are performed using the provided command line tool binaries. This mode is ideal for traditional environments where the tools are run manually or scheduled via system cron jobs.

### Backup Tool

The `backup` binary is used to create backups of BanyanDB snapshots. It connects to the gRPC endpoint of the data node to obtain snapshots and then uploads the backed-up data (organized by timestamp) to remote storage.

#### Example Backup Command

```sh
backup run \
  --grpc-addr 127.0.0.1:17912 \
  --stream-root-path /data \
  --measure-root-path /data \
  --property-root-path /data \
  --dest file:///backups \
  --time-style daily
```

**Notes:**

- `--grpc-addr`: gRPC address for the data node.
- `--stream-root-path`, `--measure-root-path`, `--property-root-path`: Local directories for the respective catalogs.
- `--dest`: Remote destination URL where backups will be stored (e.g., local filesystem path with the `file://` scheme).
- `--time-style`: Defines the time directory style, such as `daily` or `hourly`.

### Timedir Utilities

The `timedir` tool provides commands to create, read, list, and delete marker files (time directories) that indicate the backup snapshot’s timestamp. These marker files are used by the restore process to identify which snapshot to restore.

#### List Remote Timedir Files

Before creating a new local timedir file for restoration, use the timedir list command to inspect the remote backup destination. This lets you view all available timedir names and select the expected one.

```sh
restore timedir list \
  --dest file:///backups
```

Example output:

```sh
Remote time directories:
2025-02-12
2025-02-13
2025-02-14
```

Examine the list and choose the appropriate timedir (for example, 2025-02-12).

#### Creating Timedir Files

Before undertaking a pod or service restart, an administrator may create timedir marker files to capture the current backup state:

```sh
restore timedir create 2025-02-12 \
  --stream-root /data \
  --measure-root /data \
  --property-root /data
```

This command writes the specified timestamp (e.g., 2025-02-12) into files named `/data/stream/time-dir`, `/data/measure/time-dir`, and `/data/property/time-dir` in the designated root directories.

#### Verifying Timedir Files

Ensure that the timedir files have been created with the correct content by reading them:

```sh
restore timedir read \
  --stream-root /data \
  --measure-root /data \
  --property-root /data
```

The output should display the timedir content for each catalog.

### Restore Tool

**Important**: Before triggering a restore, ensure the BanyanDB data node is completely stopped.

The `restore` binary restores data from the remote backup storage onto the local machine. It examines the timedir marker files to determine which backup snapshot to apply, then synchronizes the local data directories accordingly. After a successful restore, the tool removes the timedir files to prevent unintended repeated restores.

#### Example Restore Command

```sh
restore run \
  --source file:///backups \
  --stream-root-path /data \
  --measure-root-path /data \
  --property-root-path /data
```

**Key Points:**

- The tool reads the timedir files (e.g., `/data/stream/time-dir`) to fetch the appropriate timestamp.
- Local data is compared with the remote backup snapshot; orphaned files in local directories are removed.
- Upon success, the timedir marker files are deleted to ensure a clean recovery state.

## Kubernetes Deployment

For environments running BanyanDB in Kubernetes, the backup and restore tools can be integrated as sidecar containers. A common pattern is to use an init container for restoring data and a sidecar to manage backup and timedir operations.

### Kubernetes Sidecar Deployment Model

1. **Sidecar Container:**
   - The sidecar runs the backup and timedir command binaries alongside the main BanyanDB container.
   - Administrators can attach to the sidecar (using `kubectl exec`) to execute timedir operations.
   - The sidecar is responsible for backing up snapshot data and maintaining timedir files.

2. **Init Container for Restore:**
   - When a pod is (re)created, an init container runs before the main container starts.
   - The init container uses the `restore` binary to read the timedir files from shared volumes (mounted from a persistent volume or hostPath).
   - It restores the data from the remote backup store by processing the timedir markers, and then deletes the marker files to avoid accidental re-triggering.

3. **Shared Volumes:**
   - The timedir files and data directories are shared between the init container, sidecar, and main container (data node) via Kubernetes volumes.
   - This shared setup ensures that the restore process can correctly synchronize the data state before the server starts.

By following the guidelines and examples provided in this document, administrators can choose the approach that best fits their deployment environment—whether on-prem or in Kubernetes—to ensure reliable data backup and restoration for BanyanDB.
