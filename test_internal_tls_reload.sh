#!/bin/bash
# Test script for internal TLS certificate dynamic reloading
# This script tests the dynamic reloading of CA certificates for internal TLS
# between liaison nodes and data nodes

set -e

# Create test certificate directory
rm -rf test-internal-certs && mkdir test-internal-certs && cd test-internal-certs

echo "=== Step 1: Generate initial CA certificate ==="
# Generate CA certificate
openssl req -x509 -newkey rsa:2048 -keyout ca_key1.pem -out ca_cert1.pem -days 365 -nodes -subj "/CN=TestCA1"
cp ca_cert1.pem ca_cert.pem

# Generate server certificate signed by CA
openssl genrsa -out server_key1.pem 2048
openssl req -new -key server_key1.pem -out server_csr1.pem -subj "/CN=localhost"
openssl x509 -req -in server_csr1.pem -CA ca_cert1.pem -CAkey ca_key1.pem -CAcreateserial -out server_cert1.pem -days 365
cp server_cert1.pem server_cert.pem && cp server_key1.pem server_key.pem

echo "=== Step 2: Start data node with TLS ==="
# Start data node in background
../banyand/build/bin/dev/banyand-server data \
  --tls=true \
  --cert-file=server_cert.pem \
  --key-file=server_key.pem \
  --grpc-port=17912 \
  --http-port=17913 \
  --measure-root-path=/tmp/test-data-measure \
  --stream-root-path=/tmp/test-data-stream \
  > data_node.log 2>&1 &
DATA_PID=$!

echo "Data node started with PID: $DATA_PID"
sleep 3

echo "=== Step 3: Start liaison node with internal TLS ==="
# Start liaison node with internal TLS pointing to the CA cert
../banyand/build/bin/dev/banyand-server liaison \
  --data-client-tls=true \
  --data-client-ca-cert=ca_cert.pem \
  --tls=true \
  --cert-file=server_cert.pem \
  --key-file=server_key.pem \
  --grpc-port=17914 \
  --http-port=17915 \
  > liaison_node.log 2>&1 &
LIAISON_PID=$!

echo "Liaison node started with PID: $LIAISON_PID"
sleep 5

echo "=== Step 4: Verify initial connection ==="
# Check if liaison can connect to data node
echo "Checking initial connection..."
if grep -q "new node is healthy" liaison_node.log; then
    echo "✓ Initial connection successful"
else
    echo "✗ Initial connection failed"
    kill $DATA_PID $LIAISON_PID 2>/dev/null || true
    exit 1
fi

echo "=== Step 5: Generate new CA certificate ==="
# Generate new CA certificate
openssl req -x509 -newkey rsa:2048 -keyout ca_key2.pem -out ca_cert2.pem -days 365 -nodes -subj "/CN=TestCA2"

# Generate new server certificate signed by new CA
openssl genrsa -out server_key2.pem 2048
openssl req -new -key server_key2.pem -out server_csr2.pem -subj "/CN=localhost"
openssl x509 -req -in server_csr2.pem -CA ca_cert2.pem -CAkey ca_key2.pem -CAcreateserial -out server_cert2.pem -days 365

# Update CA cert file (this should trigger reload on liaison)
cp ca_cert2.pem ca_cert.pem

# Update server cert and key (this should trigger reload on data node if it supports it)
cp server_cert2.pem server_cert.pem && cp server_key2.pem server_key.pem

echo "=== Step 6: Wait for certificate reload ==="
sleep 3

echo "=== Step 7: Verify connection after certificate update ==="
# Check if liaison reconnected after CA cert update
if grep -q "CA certificate updated" liaison_node.log && grep -q "successfully reconnected client after CA certificate update" liaison_node.log; then
    echo "✓ CA certificate reload and reconnection successful"
else
    echo "✗ CA certificate reload or reconnection failed"
    echo "Liaison log:"
    tail -20 liaison_node.log
    kill $DATA_PID $LIAISON_PID 2>/dev/null || true
    exit 1
fi

echo "=== Step 8: Cleanup ==="
kill $DATA_PID $LIAISON_PID 2>/dev/null || true
wait $DATA_PID $LIAISON_PID 2>/dev/null || true

echo "=== Test completed successfully ==="
cd ..
rm -rf test-internal-certs

