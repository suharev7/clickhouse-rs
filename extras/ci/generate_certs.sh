#!/usr/bin/env bash

crt=$CH_SSL_CERTIFICATE
key=$CH_SSL_PRIVATE_KEY

openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout "$key" -out "$crt"
chown clickhouse:clickhouse "$crt" "$key"
