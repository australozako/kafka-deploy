#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/mnt/c/Users/sirib/kafka-deploy"
cd "$PROJECT_DIR"

TARGET_PHASE="${1:-}"
SSL_CONF="/etc/kafka/secrets/client-ssl.properties"
BOOTSTRAP="kafka-1:29092,kafka-2:29092,kafka-3:29092"
CONTROLLERS="kafka-controller-1:9093,kafka-controller-2:9093,kafka-controller-3:9093"

if [[ ! "$TARGET_PHASE" =~ ^(phase2|phase3)$ ]]; then
  echo "Usage: $0 <phase2|phase3>"
  exit 1
fi

wait_for_quorum_leader() {
  local node="$1"
  echo "⏳ Attente de stabilisation du quorum KRaft après redémarrage de $node..."
  for attempt in $(seq 1 30); do
    local check_ctrl="kafka-controller-1"
    [[ "$node" == "kafka-controller-1" ]] && check_ctrl="kafka-controller-2"

    if docker exec "$check_ctrl" kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status 2>/dev/null \
      | grep -Eq "LeaderId:\s*(101|102|103)"; then
      echo "✅ [OK] Quorum KRaft actif et synchronisé après redémarrage de $node."
      return 0
    fi
    sleep 2
  done
  echo "❌ [KO] Timeout quorum KRaft après redémarrage de $node."
  exit 1
}

wait_for_zero_urp() {
  local current_broker="$1"
  echo "⏳ Attente de la resynchronisation complète des partitions (URP = 0) après redémarrage de $current_broker..."
  for attempt in $(seq 1 30); do
    local check_node="kafka-1"
    [[ "$current_broker" == "kafka-1" ]] && check_node="kafka-2"

    URP=$(docker exec "$check_node" kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --describe --under-replicated-partitions 2>/dev/null | grep -c "Partition:" || true)

    if [[ "$URP" -eq 0 ]]; then
      echo "✅ [OK] Cluster synchronisé (0 partition sous-répliquée). $current_broker est opérationnel."
      return 0
    fi
    echo "   Tentative $attempt/30 : $URP partition(s) encore en cours de réplication..."
    sleep 2
  done
  echo "❌ [KO] Timeout resynchronisation ISR après redémarrage de $current_broker."
  exit 1
}

echo "=================================================================="
echo "  DÉMARRAGE DU ROLLING RESTART VERS ${TARGET_PHASE^^} (ZÉRO COUPURE)"
echo "=================================================================="

# 1. Rendu du docker-compose.yml cible
CLUSTER_ID=$(cat "$PROJECT_DIR/.cluster_id")
ansible localhost -i inventory.ini -m ansible.builtin.template \
  -a "src=templates/docker-compose.yml.j2 dest=$PROJECT_DIR/docker-compose.yml mode=0644" \
  -e "@group_vars/all.yml" \
  -e "migration_phase=$TARGET_PHASE detected_cluster_id=$CLUSTER_ID" >/dev/null
echo "✅ [OK] Fichier docker-compose.yml généré pour $TARGET_PHASE (CLUSTER_ID: $CLUSTER_ID)"

# 2. En Phase 3, redémarrer d'abord les contrôleurs un par un (retrait du mode migration)
if [[ "$TARGET_PHASE" == "phase3" ]]; then
  for i in 1 2 3; do
    CTRL="kafka-controller-${i}"
    echo ""
    echo "🔄 [Rolling Restart Contrôleur $i/3] Mise à jour de $CTRL..."
    docker compose up -d --no-deps "$CTRL"
    sleep 5
    wait_for_quorum_leader "$CTRL"
  done
fi

# 3. Rolling Restart séquentiel des 3 brokers (kafka-1 -> kafka-2 -> kafka-3)
for i in 1 2 3; do
  BROKER="kafka-${i}"
  echo ""
  echo "🔄 [Rolling Restart Broker $i/3] Mise à jour de $BROKER..."
  docker compose up -d --no-deps "$BROKER"
  sleep 4
  wait_for_zero_urp "$BROKER"
done

# 4. Mise à jour Kafbat UI et suppression des orphelins (ZooKeeper en phase3)
echo ""
echo "🔄 Mise à jour de Kafbat UI et nettoyage des conteneurs orphelins..."
docker compose up -d --remove-orphans

# 5. Attente active de la convergence post-migration (DUAL_WRITE en phase2 / 3 Observers en phase3)
echo "⏳ Vérification de la convergence finale de $TARGET_PHASE..."
for attempt in $(seq 1 20); do
  if [[ "$TARGET_PHASE" == "phase2" ]]; then
    QUORUM=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status 2>/dev/null || true)
    if echo "$QUORUM" | grep -q '"id": 1,' && echo "$QUORUM" | grep -q '"id": 2,' && echo "$QUORUM" | grep -q '"id": 3,'; then
      echo "✅ [OK] Les 3 brokers (1, 2, 3) sont enregistrés en Dual Write (Observers) dans le quorum KRaft."
      echo "🎉 ROLLING RESTART VERS PHASE2 TERMINÉ SANS INTERRUPTION DE SERVICE !"
      exit 0
    fi
  elif [[ "$TARGET_PHASE" == "phase3" ]]; then
    QUORUM=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status 2>/dev/null || true)
    if echo "$QUORUM" | grep -q '"id": 1,' && echo "$QUORUM" | grep -q '"id": 2,' && echo "$QUORUM" | grep -q '"id": 3,'; then
      echo "✅ [OK] Les 3 brokers (1, 2, 3) sont enregistrés en tant qu'Observers dans le quorum KRaft."
      echo "🎉 ROLLING RESTART VERS PHASE3 TERMINÉ SANS INTERRUPTION DE SERVICE !"
      exit 0
    fi
  fi
  sleep 2
done

echo "❌ [KO] Timeout lors de l'attente de convergence de $TARGET_PHASE."
exit 1
