# 📄 TLS Certificates for Integration Tests

This folder contains the configuration and setup instructions for generating self-signed TLS certificates used in integration tests.

## 🛠 How to Generate TLS Certs

1. Navigate to this directory:

    ```bash
    cd test/testdata/certs


2. Run this command to generate the certificate and key using the cert.conf file:

    ```bash
    openssl req -x509 -newkey rsa:4096 -nodes \
    -keyout server.key -out server.crt \
    -days 365 -config cert.conf -extensions v3_req