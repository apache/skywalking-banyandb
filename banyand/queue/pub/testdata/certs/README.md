# TLS Test Certificates

This folder contains everything you need to generate a self‑signed CA and a server certificate that’s valid for both `localhost` and your machine’s real hostname.

## Prerequisites

- Bash (or any POSIX shell)
- OpenSSL (v1.1.1 or later)
- `envsubst` (part of GNU gettext)

## Steps

1. **Set the HOSTNAME variable**
   ```bash
   export HOSTNAME=$(hostname)
2. **Generate your test CA (valid for 10 years)**

   ```bash
   openssl req \
   -x509 -nodes \
   -newkey rsa:4096 \
   -days 3650 \
   -keyout ca.key \
   -out ca.crt \
   -subj "/CN=MyTestCA"

3. Render the OpenSSL config with your hostname, and create a CSR + key:
   ```bash
   sed "s/\${HOSTNAME}/$HOSTNAME/g" cert.conf.template > cert.conf
   ```
   ```bash
   openssl req \
   -newkey rsa:2048 -nodes \
   -keyout server.key \
   -out server.csr \
   -config cert.conf
   
4. Sign the server CSR with your CA (valid for 1 year):
   ```bash
   openssl x509 \
   -req \
   -in server.csr \
   -CA ca.crt \
   -CAkey ca.key \
   -CAcreateserial \
   -out server.crt \
   -days 365 \
   -sha256 \
   -extensions v3_req \
   -extfile cert.conf
   
5. Clean up (optional):
   ```bash
   rm cert.conf server.csr ca.srl