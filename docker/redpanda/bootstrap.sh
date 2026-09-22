#!/usr/bin/env bash
# Create the swarm topics. Idempotent: existing topics are left untouched.
#
# Reads docker/redpanda/topics.yaml. Works against Redpanda (rpk) and, with
# KAFKA_TOPICS_SH set, against Apache Kafka (kafka-topics.sh).
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-localhost:19092}"
TOPICS_FILE="${TOPICS_FILE:-$(dirname "$0")/topics.yaml}"

log() { printf '[bootstrap] %s\n' "$*"; }

# Minimal YAML reader: pulls name/partitions/replicas/retention_ms per entry.
parse_topics() {
  awk '
    /^[[:space:]]*-[[:space:]]*name:/ {
      if (name != "") print name, parts, reps, ret
      name = $NF; parts = 1; reps = 1; ret = 604800000
      next
    }
    /^[[:space:]]*partitions:/   { parts = $NF }
    /^[[:space:]]*replicas:/     { reps  = $NF }
    /^[[:space:]]*retention_ms:/ { ret   = $NF }
    END { if (name != "") print name, parts, reps, ret }
  ' "$TOPICS_FILE"
}

create_with_rpk() {
  local name=$1 parts=$2 reps=$3 ret=$4
  if rpk topic describe "$name" --brokers "$BROKERS" >/dev/null 2>&1; then
    log "exists   $name"
  else
    rpk topic create "$name" \
      --brokers "$BROKERS" \
      --partitions "$parts" \
      --replicas "$reps" \
      --topic-config "retention.ms=$ret" \
      --topic-config "cleanup.policy=delete"
    log "created  $name (partitions=$parts replicas=$reps)"
  fi
}

create_with_kafka_sh() {
  local name=$1 parts=$2 reps=$3 ret=$4
  "$KAFKA_TOPICS_SH" --bootstrap-server "$BROKERS" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor "$reps" \
    --config "retention.ms=$ret" \
    --config "cleanup.policy=delete"
  log "ensured  $name"
}

log "brokers=$BROKERS topics=$TOPICS_FILE"

while read -r name parts reps ret; do
  [ -z "${name:-}" ] && continue
  if [ -n "${KAFKA_TOPICS_SH:-}" ]; then
    create_with_kafka_sh "$name" "$parts" "$reps" "$ret"
  else
    create_with_rpk "$name" "$parts" "$reps" "$ret"
  fi
done < <(parse_topics)

log "done"
if [ -z "${KAFKA_TOPICS_SH:-}" ]; then
  rpk topic list --brokers "$BROKERS"
fi
