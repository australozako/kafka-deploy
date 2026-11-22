#!/usr/bin/env bash
set -euo pipefail

PHASE="${1:-}"
SSL_CONF="/etc/kafka/secrets/client-ssl.properties"

pass() { echo "✅ [OK] $1"; }
fail() { echo "❌ [KO] $1"; exit 1; }

case "$PHASE" in
  phase0)
    echo "=== VALIDATION SINGLE-NODE PHASE 0 (1 ZooKeeper + 1 Broker) ==="
    docker ps --format '{{.Names}}' | grep -q "^zookeeper$" && pass "zookeeper actif" || fail "zookeeper absent"
    docker ps --format '{{.Names}}' | grep -q "^kafka$" && pass "kafka actif" || fail "kafka absent"
    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --command-config "$SSL_CONF" \
      --create --if-not-exists --topic baseline-zk-single --partitions 1 --replication-factor 1 >/dev/null
    seq 1 5 | sed 's/^/msg-single-/' | docker exec -i kafka kafka-console-producer \
      --bootstrap-server kafka:29092 --producer.config "$SSL_CONF" --topic baseline-zk-single >/dev/null
    pass "5 messages produits dans baseline-zk-single (mTLS)"
    ;;
  phase1)
    echo "=== VALIDATION SINGLE-NODE PHASE 1 (Quorum KRaft initialisé) ==="
    docker exec kafka-controller kafka-metadata-quorum \
      --bootstrap-controller kafka-controller:9093 --command-config "$SSL_CONF" describe --status \
      | grep -Eq "LeaderId:\s*101" && pass "Contrôleur KRaft (ID 101) Leader actif" || fail "Contrôleur KRaft non prêt"
    ;;
  phase2)
    echo "=== VALIDATION SINGLE-NODE PHASE 2 (Dual Write) ==="
    docker exec kafka-controller kafka-metadata-quorum \
      --bootstrap-controller kafka-controller:9093 --command-config "$SSL_CONF" describe --status \
      | grep -q '"id": 1,' && pass "Broker (ID 1) enregistré en Observer (Dual Write)" || fail "Broker non enregistré en Dual Write"
    ;;
  phase3)
    echo "=== VALIDATION SINGLE-NODE PHASE 3 (KRaft Pur sans ZooKeeper) ==="
    docker ps -a --format '{{.Names}}' | grep -q "^zookeeper$" && fail "zookeeper encore présent" || pass "zookeeper supprimé"
    COUNT=$(docker exec kafka kafka-console-consumer \
      --bootstrap-server kafka:29092 --consumer.config "$SSL_CONF" \
      --topic baseline-zk-single --from-beginning --max-messages 5 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$COUNT" -eq 5 ]] && pass "Données historiques intactes sans ZooKeeper ($COUNT/5 messages)" || fail "Perte de messages ($COUNT/5)"
    ;;
  *)
    echo "Usage: $0 <phase0|phase1|phase2|phase3>"
    exit 1
    ;;
esac
