#!/usr/bin/env bash
set -euo pipefail

purge_all_kafka_stack() {
  echo "🧹 Purge complète des conteneurs, réseaux et volumes Kafka / ZooKeeper / Kafbat..."
  CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^(zookeeper|kafka|kafbat-ui)' || true)
  [[ -n "$CONTAINERS" ]] && echo "$CONTAINERS" | xargs -r docker rm -f >/dev/null
  sleep 1
  VOLUMES=$(docker volume ls --format '{{.Name}}' | grep -E '(zookeeper|kafka|zk_data|zk_log|broker_data|controller_data)' || true)
  [[ -n "$VOLUMES" ]] && echo "$VOLUMES" | xargs -r docker volume rm -f >/dev/null
  docker network ls --format '{{.Name}}' | grep -E '(kafka-deploy|single-node)' | xargs -r docker network rm >/dev/null 2>&1 || true
  rm -f /mnt/c/Users/sirib/kafka-deploy/.cluster_id /mnt/c/Users/sirib/kafka-deploy/single-node/.cluster_id
}


PROJECT_DIR="/mnt/c/Users/sirib/kafka-deploy"
cd "$PROJECT_DIR"

MODE="${1:-}"

destroy_all() {
  echo "=== DESTRUCTION COMPLÈTE DU CLUSTER HA (3-3-3) ==="
  if [[ -f docker-compose.yml ]]; then
  purge_all_kafka_stack
  fi
  # Suppression explicite de tous les conteneurs et volumes résiduels
  docker rm -f \
    zookeeper-1 zookeeper-2 zookeeper-3 \
    kafka-1 kafka-2 kafka-3 \
    kafka-controller-1 kafka-controller-2 kafka-controller-3 \
    kafbat-ui 2>/dev/null || true

  docker volume ls -q | grep -E "zk_data_|zk_log_|controller_data_|broker_data_" | xargs -r docker volume rm -f || true
  rm -f "$PROJECT_DIR/.cluster_id" "$PROJECT_DIR/docker-compose.yml"
  echo "✅ [OK] Environnement HA totalement remis à zéro (9 conteneurs, 12 volumes et .cluster_id supprimés)."
}

run_phase() {
  local phase="$1"
  echo ""
  echo "=================================================="
  echo ">>> EXÉCUTION DE LA ${phase^^} (CLUSTER 3-3-3)"
  echo "=================================================="
  ansible-playbook -i inventory.ini playbook.yml -e "migration_phase=${phase}"
  ./validate-migration.sh "${phase}"
}

case "$MODE" in
  destroy)
    destroy_all
    ;;
  full-cycle)
    destroy_all
    run_phase "phase0"
    run_phase "phase1"
    run_phase "phase2"
    run_phase "phase3"
    echo ""
    echo "🎉 CYCLE COMPLET HA (Phase 0 -> Phase 3 + Test Failover) EXÉCUTÉ ET VALIDÉ !"
    ;;
  *)
    echo "Usage: $0 <destroy|full-cycle>"
    echo "  destroy    : Supprime les 9 conteneurs, 12 volumes et le cache .cluster_id"
    echo "  full-cycle : Remet tout à zéro puis exécute et valide Phase 0 -> 1 -> 2 -> 3 + Failover"
    exit 1
    ;;
esac
