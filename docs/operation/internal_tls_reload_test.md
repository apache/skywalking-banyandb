# Internal TLS Certificate Dynamic Reloading Test Guide

This document describes how to test the dynamic reloading of certificates for internal TLS communication between liaison nodes and data nodes.

## Overview

The internal TLS dynamic reloading feature allows certificates used for internal TLS communication to be updated without restarting the service:

- **Liaison nodes**: CA certificate (`--data-client-ca-cert`) used to verify data node server certificates, and server certificates (`--cert-file`, `--key-file`) used for serving clients
- **Data nodes**: Server certificates (`--cert-file`, `--key-file`) used for serving liaison node clients

This is similar to the external TLS certificate dynamic reloading feature implemented in #12862.

## Test Scenario

### Prerequisites

1. Build the BanyanDB server:
   ```bash
   make build
   ```

2. Prepare test certificates directory:
   ```bash
   mkdir -p test-internal-certs && cd test-internal-certs
   ```

### Step 1: Generate Initial CA and Server Certificates

**Important**: The server certificate's Common Name (CN) or Subject Alternative Name (SAN) must match the hostname used when nodes register to etcd. If your nodes register with a hostname like `master01` or `node1`, the certificate must include that hostname. If you use `localhost`, ensure all connections use `localhost` as well.

**Generate certificate with hostname as CN (if you know the hostname)**

```bash
# Generate CA certificate
openssl req -x509 -newkey rsa:2048 -keyout ca_key1.pem -out ca_cert1.pem -days 365 -nodes -subj "/CN=TestCA1"
cp ca_cert1.pem ca_cert.pem

# Generate server certificate signed by CA
# Replace 'master01' with your actual hostname
openssl genrsa -out server_key1.pem 2048
openssl req -new -key server_key1.pem -out server_csr1.pem -subj "/CN=master01"
openssl x509 -req -in server_csr1.pem -CA ca_cert1.pem -CAkey ca_key1.pem -CAcreateserial -out server_cert1.pem -days 365
cp server_cert1.pem server_cert.pem && cp server_key1.pem server_key.pem
```

### Step 2: Start Data Node with TLS

```bash
CERTS_DIR=/data/wangyi/skywalking-banyandb/test-internal-certs
./banyand/build/bin/dev/banyand-server data \
  --etcd-endpoints=http://127.0.0.1:2379 \
  --tls=true \
  --cert-file=$CERTS_DIR/server_cert.pem \
  --key-file=$CERTS_DIR/server_key.pem \
  --grpc-port=17912 \
  --http-port=17913 \
  --measure-root-path=/tmp/test-data-measure \
  --stream-root-path=/tmp/test-data-stream
```

### Step 3: Start Liaison Node with Internal TLS

```bash
CERTS_DIR=/data/wangyi/skywalking-banyandb/test-internal-certs
./banyand/build/bin/dev/banyand-server liaison \
  --etcd-endpoints=http://127.0.0.1:2379 \
  --data-client-tls=true \
  --data-client-ca-cert=$CERTS_DIR/ca_cert.pem \
  --tls=true \
  --cert-file=$CERTS_DIR/server_cert.pem \
  --key-file=$CERTS_DIR/server_key.pem \
  --grpc-port=17914 \
  --http-port=17915
```

**Note**: The flag names are `--data-client-tls` and `--data-client-ca-cert` (not `--internal-tls` and `--internal-ca-cert`). The prefix "data" comes from the role of the target nodes (data nodes).

### Step 4: Verify Initial Connection

Check the liaison node logs to verify that it successfully connected to the data node:

```bash
# Look for messages like:
# "new node is healthy, add it to active queue"
# "Initialized CA certificate reloader"
# "Started CA certificate file monitoring"
```

### Step 5: Generate New Certificates

**Generate certificate with hostname as CN**

```bash
# Generate new CA certificate
openssl req -x509 -newkey rsa:2048 -keyout ca_key2.pem -out ca_cert2.pem -days 365 -nodes -subj "/CN=TestCA2"

# Generate new server certificate signed by new CA
# Replace 'master01' with your actual hostname
openssl genrsa -out server_key2.pem 2048
openssl req -new -key server_key2.pem -out server_csr2.pem -subj "/CN=master01"
openssl x509 -req -in server_csr2.pem -CA ca_cert2.pem -CAkey ca_key2.pem -CAcreateserial -out server_cert2.pem -days 365

# Update CA cert file (this should trigger reload on liaison for client connections)
cp ca_cert2.pem ca_cert.pem

# Update server cert and key (this should trigger reload on both liaison and data nodes)
cp server_cert2.pem server_cert.pem && cp server_key2.pem server_key.pem
```

**Note**: When updating server certificates, you need to update them on both liaison and data nodes if they are using the same certificate files. The reloader will automatically detect the changes and reload the certificates.

### Step 6: Verify Certificate Reload

Wait a few seconds (the reloader has a 500ms debounce), then check the logs:

**Liaison node logs:**
```bash
# For CA certificate reload:
# "CA certificate updated, reconnecting clients"
# "successfully reconnected client after CA certificate update"

# For server certificate reload:
# "Successfully updated TLS certificate after content change"
# "TLS certificate updated in memory"
# "Starting TLS file monitoring"
```

**Data node logs:**
```bash
# Look for messages like:
# "Successfully updated TLS certificate after content change"
# "TLS certificate updated in memory"
# "Started TLS file monitoring for queue server"
```

### Step 7: Verify Connection Still Works

After the CA certificate is updated, the liaison node should automatically reconnect to the data node using the new CA certificate. Verify that:

1. The liaison node logs show successful reconnection
2. Data can still be written from liaison to data nodes
3. No connection errors occur

## Expected Behavior

1. **Initial Connection**: Liaison node connects to data node using the initial CA certificate
2. **CA Certificate Update (Liaison)**: When the CA certificate file (`--data-client-ca-cert`) is updated on liaison nodes, the reloader detects the change and automatically reconnects all clients to data nodes
3. **Server Certificate Update (Liaison)**: When the server certificate files (`--cert-file`, `--key-file`) are updated on liaison nodes, the reloader detects the change and automatically reloads the certificates for serving external clients
4. **Server Certificate Update (Data)**: When the server certificate files (`--cert-file`, `--key-file`) are updated on data nodes, the reloader detects the change and automatically reloads the certificates for serving liaison node clients
5. **No Service Interruption**: All certificate updates happen automatically without requiring service restarts

## Differences from External TLS Reloading

- **External TLS** (#12862): Reloads server certificates and keys for client-to-server communication (liaison/standalone as servers for external clients)
- **Internal TLS** (this feature): 
  - Reloads CA certificates on liaison nodes (for verifying data node server certificates when connecting as clients)
  - Reloads server certificates and keys on liaison nodes (for serving external clients, same certificates as external TLS)
  - Reloads server certificates and keys on data nodes (for serving liaison node clients)

## Troubleshooting

1. **Connection fails with "node is unhealthy" or "failed to re-connect to grpc server"**:
   - **Most common cause**: Certificate hostname mismatch. The server certificate's CN or SAN must match the hostname used when nodes register to etcd.
   - Check the node registration address in etcd (usually shown in logs as `grpc_address` like `master01:17912`)
   - Verify the certificate includes the correct hostname:
     ```bash
     openssl x509 -in server_cert.pem -text -noout | grep -A 2 "Subject:"
     openssl x509 -in server_cert.pem -text -noout | grep -A 5 "Subject Alternative Name"
     ```
   - If the hostname doesn't match, regenerate the certificate with the correct CN or add SAN extension
   - Ensure the CA certificate can verify the server certificate:
     ```bash
     openssl verify -CAfile ca_cert.pem server_cert.pem
     ```

2. **Connection fails after CA cert update**: 
   - Ensure the new CA certificate can verify the data node's server certificate
   - Check that the server certificate is signed by the new CA

3. **No reload detected**:
   - Verify file permissions (should be readable)
   - Check that the file is actually updated (not just touched)
   - Look for errors in the liaison node logs

4. **Reconnection fails**:
   - Verify the data node is still running
   - Check that the new CA certificate is valid
   - Ensure the server certificate matches the new CA
   - Check for hostname mismatch issues (see #1)

5. **Data node certificate reload fails**:
   - Verify file permissions (should be readable)
   - Check that both cert and key files are updated
   - Look for errors in the data node logs
   - Ensure the new certificate and key are valid and match each other

6. **Liaison server certificate reload fails**:
   - Verify file permissions (should be readable)
   - Check that both cert and key files are updated
   - Look for errors in the liaison node logs
   - Ensure the new certificate and key are valid and match each other
   - Note: This affects external client connections, not internal connections to data nodes

