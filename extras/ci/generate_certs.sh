#!/usr/bin/env bash

out=$1 && shift
mkdir -p "$out"
cd "$out"

#
# CA
#
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.pem -subj "/C=US/ST=DevState/O=DevOrg/CN=MyDevCA"

#
# server
#
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/C=US/ST=DevState/O=DevOrg/CN=localhost"

cat > server.ext <<EOL
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
EOL

openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.crt -days 825 -sha256 -extfile server.ext
openssl verify -CAfile ca.pem server.crt

#
# client
#
cat > client.ext <<EOL
basicConstraints=CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
EOL

openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -subj "/C=US/ST=DevState/O=DevOrg/CN=MyClient"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256 -extfile client.ext
openssl verify -CAfile ca.pem client.crt

# server needs access to those
chmod 644 ca.pem server.key server.crt
