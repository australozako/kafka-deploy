#!/usr/bin/env bash
set -euo pipefail

SINGLE_DIR="/mnt/c/Users/sirib/kafka-deploy/single-node"
ROOT_DIR="/mnt/c/Users/sirib/kafka-deploy"
cd "$SINGLE_DIR"

MODE="${1:-full-cycle}"

purge_all_kafka_stack() {
  echo "=== DESTRUCTION COMPLÈTE DU CLUSTER (HA & SINGLE-NODE) ==="
  # 1. Suppression forcée de tous les conteneurs Kafka / ZooKeeper / Kafbat UI
  CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^(zookeeper|kafka|kafbat-ui)' || true)
  if [[ -n "$CONTAINERS" ]]; then
    echo "$CONTAINERS" | xargs -r docker rm -f >/dev/null
  fi
  sleep 1

  # 2. Suppression de tous les volumes associés une fois les conteneurs détruits
  VOLUMES=$(docker volume ls --format '{{.Name}}' | grep -E '(zookeeper|kafka|zk_data|zk_log|broker_data|controller_data)' || true)
  if [[ -n "$VOLUMES" ]]; then
    echo "$VOLUMES" | xargs -r docker volume rm -f >/dev/null
  fi

  # 3. Nettoyage des réseaux et fichiers d'état
  docker network ls --format '{{.Name}}' | grep -E '(kafka-deploy|single-node)' | xargs -r docker network rm >/dev/null 2>&1 || true
  rm -f "$ROOT_DIR/.cluster_id" "$SINGLE_DIR/.cluster_id"
  echo "✅ [OK] Environnement totalement remis à zéro (conteneurs, volumes et .cluster_id supprimés)."
}

run_phase() {
  local phase="$1"
  echo ""
  echo "=================================================="
  echo ">>> EXÉCUTION DE LA ${phase^^} (SINGLE-NODE)"
  echo "=================================================="
  ansible-playbook -i "$ROOT_DIR/inventory.ini" "$SINGLE_DIR/playbook-single.yml" \
    -e "@$ROOT_DIR/group_vars/all.yml" \
    -e "migration_phase=$phase"
  "$SINGLE_DIR/validate-single.sh" "$phase"
}

case "$MODE" in
  destroy)
    purge_all_kafka_stack
    ;;
  full-cycle)
    purge_all_kafka_stack
    for p in phase0 phase1 phase2 phase3; do
      run_phase "$p"
    done
    echo ""
    echo "🎉 CYCLE COMPLET SINGLE-NODE (PHASE 0 -> PHASE 3) TERMINÉ AVEC SUCCÈS !"
    ;;
  phase0|phase1|phase2|phase3)
    run_phase "$MODE"
    ;;
  *)
    echo "Usage: $0 {destroy|full-cycle|phase0|phase1|phase2|phase3}"
    exit 1
    ;;
esac
