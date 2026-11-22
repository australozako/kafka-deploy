#!/usr/bin/env bash
set -euo pipefail

SSL_CONF="/etc/kafka/secrets/client-ssl.properties"
BOOTSTRAP="kafka-1:29092,kafka-2:29092,kafka-3:29092"
CONTROLLERS="kafka-controller-1:9093,kafka-controller-2:9093,kafka-controller-3:9093"
TOPIC="live-migration-canary"
LOG_DIR="/tmp/kafka-live-monitor"

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

cleanup() {
  echo ""
  echo "Arrêt du moniteur live..."
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "=== INITIALISATION DU CANARY DE SURVEILLANCE LIVE ==="
docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 3 >/dev/null

# 1. Producteur continu en arrière-plan (1 msg / 0.5s avec acks=all)
(
  seq_num=1
  while true; do
    ts=$(date +"%H:%M:%S.%3N")
    echo "canary-${seq_num}|${ts}"
    seq_num=$((seq_num + 1))
    sleep 0.5
  done | docker exec -i kafka-1 kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP" \
    --producer.config "$SSL_CONF" \
    --producer-property acks=all \
    --producer-property retries=10 \
    --producer-property retry.backoff.ms=500 \
    --topic "$TOPIC" >> "$LOG_DIR/producer.log" 2>&1
) &

# 2. Consommateur continu en arrière-plan
(
  docker exec kafka-2 kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP" \
    --consumer.config "$SSL_CONF" \
    --topic "$TOPIC" \
    --group "live-canary-group-$(date +%s)" >> "$LOG_DIR/consumer.log" 2>&1
) &

sleep 2

# 3. Tableau de bord temps réel
while true; do
  clear
  echo "=============================================================================="
  echo "  SURVEILLANCE LIVE DU CLUSTER KAFKA (mTLS | acks=all | RF=3) - $(date +%H:%M:%S)"
  echo "=============================================================================="

  # État des conteneurs
  printf "\n[1] ÉTAT DES NŒUDS :\n"
  docker ps --format "  - {{.Names}}\t({{.Status}})" | grep -E "kafka-|zookeeper-" | sort

  # Quorum KRaft (si actif)
  printf "\n[2] QUORUM KRAFT :\n"
  if docker ps --format '{{.Names}}' | grep -q "^kafka-controller-1$"; then
    docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status 2>/dev/null \
      | grep -E "LeaderId:|CurrentVoters:|CurrentObservers:|MaxFollowerLag:" | sed 's/^/  /' || echo "  En cours d'élection..."
  else
    echo "  Contrôleurs KRaft non démarrés (Phase 0)"
  fi

  # Santé des partitions (Under-Replicated Partitions)
  printf "\n[3] SANTÉ DES PARTITIONS (URP) :\n"
  URP_COUNT=$(docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
    --describe --under-replicated-partitions 2>/dev/null | wc -l)
  if [[ "$URP_COUNT" -eq 0 ]]; then
    echo "  ✅ 0 partition sous-répliquée (Cluster 100% synchronisé)"
  else
    echo "  ⚠️  $URP_COUNT partition(s) en cours de réplication (ISR dégradé temporaire)"
  fi

  # Trafic Canary Live
  printf "\n[4] FLUX TEMPS RÉEL (PRODUCER acks=all -> CONSUMER) :\n"
  LAST_MSG=$(tail -n 1 "$LOG_DIR/consumer.log" 2>/dev/null || echo "En attente...")
  TOTAL_READ=$(wc -l < "$LOG_DIR/consumer.log" 2>/dev/null || echo 0)
  PROD_ERR=$(grep -icE "ERROR|Exception" "$LOG_DIR/producer.log" 2>/dev/null || true)
  echo "  Messages reçus en direct : $TOTAL_READ | Erreurs critiques producer : $PROD_ERR"
  echo "  Dernier message reçu     : $LAST_MSG"
  echo "=============================================================================="
  echo "Appuyez sur Ctrl+C pour arrêter la surveillance."
  sleep 2
done
