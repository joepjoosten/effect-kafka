#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Only the dedicated benchmark project is reset. Existing benchmark data is disposable.
docker compose -p effect-kafka-benchmark -f benchmarks/compose.yml down --volumes --remove-orphans
bash benchmarks/certificates.sh
docker compose -p effect-kafka-benchmark -f benchmarks/compose.yml up -d --wait --wait-timeout 180
docker compose -p effect-kafka-benchmark -f benchmarks/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 --alter \
  --entity-type users --entity-name benchmark \
  --add-config 'SCRAM-SHA-256=[iterations=4096,password=benchmark-secret]'
