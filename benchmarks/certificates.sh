#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p .benchmark/certs
openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
  -config benchmarks/cert.conf -keyout .benchmark/certs/key.pem -out .benchmark/certs/ca.pem
cat .benchmark/certs/key.pem .benchmark/certs/ca.pem > .benchmark/certs/broker.pem
# Throwaway localhost test key; the unprivileged Docker broker must be able to read it.
chmod 644 .benchmark/certs/broker.pem .benchmark/certs/ca.pem
