#!/usr/bin/env bash
set -euo pipefail

PHASE="${1:-}"
SSL_CONF="/etc/kafka/secrets/client-ssl.properties"
BOOTSTRAP="localhost:9092"
CONTROLLER="localhost:9093"

if [[ -z "$PHASE" ]]; then
  echo "Usage: $0 <phase0|phase1|phase2|phase3|cleanup>"
  exit 1
fi

pass() { echo -e "✅ [OK] $1"; }
fail() { echo -e "❌ [KO] $1"; exit 1; }

case "$PHASE" in
  phase0)
    echo "=== VALIDATION PHASE 0 (Baseline ZooKeeper) ==="
    docker ps --format '{{.Names}}' | grep -q "^zookeeper$" && pass "ZooKeeper actif" || fail "ZooKeeper absent"
    docker ps --format '{{.Names}}' | grep -q "^kafka$" && pass "Broker Kafka actif" || fail "Broker Kafka absent"

    CID=$(docker exec zookeeper zookeeper-shell localhost:2181 get /cluster/id 2>/dev/null | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    [[ -n "$CID" ]] && pass "CLUSTER_ID récupéré : $CID" || fail "CLUSTER_ID introuvable"

    docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic baseline-zk-topic --partitions 3 --replication-factor 1 >/dev/null
    seq 1 10 | sed 's/^/message-zk-/' | docker exec -i kafka kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" --producer.config "$SSL_CONF" --topic baseline-zk-topic >/dev/null
    pass "Topic baseline-zk-topic créé et 10 messages produits en mTLS"
    ;;

  phase1)
    echo "=== VALIDATION PHASE 1 (Contrôleur KRaft en attente) ==="
    docker exec kafka-controller kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLER" --command-config "$SSL_CONF" describe --status | grep -q "LeaderId:.*101" \
      && pass "Quorum KRaft actif (LeaderId: 101)" || fail "Quorum KRaft non prêt"

    docker logs kafka-controller 2>&1 | grep -qi "Waiting for brokers to register" \
      && pass "Contrôleur en attente d'enregistrement du broker ZK" || fail "Message d'attente broker absent"
    ;;

  phase2)
    echo "=== VALIDATION PHASE 2 (Mode Dual Write) ==="
    docker logs kafka-controller --tail 100 2>&1 | grep -q "to DUAL_WRITE state" \
      && pass "État DUAL_WRITE actif sur le contrôleur" || fail "Contrôleur pas en état DUAL_WRITE"

    if docker logs kafka-controller --tail 50 2>&1 | grep -Eq "SSL handshake failed|PKIX"; then
      fail "Erreurs SSL détectées dans les logs du contrôleur"
    else
      pass "Aucune erreur SSL/PKIX récente"
    fi

    QUORUM=$(docker exec kafka-controller kafka-metadata-quorum --bootstrap-controller "$CONTROLLER" --command-config "$SSL_CONF" describe --status)
    echo "$QUORUM" | grep -q '"id": 1' \
      && pass "Broker ID 1 rattaché en tant qu'Observer dans le quorum" || fail "Broker ID 1 absent des Observers"

    docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic test-dual-write-sync --partitions 2 --replication-factor 1 >/dev/null
    sleep 2
    docker exec zookeeper zookeeper-shell localhost:2181 ls /brokers/topics 2>/dev/null | grep -q "test-dual-write-sync" \
      && pass "Synchronisation KRaft -> ZooKeeper validée (topic visible dans ZK)" || fail "Topic non répliqué dans ZooKeeper"

    seq 1 5 | sed 's/^/msg-dual-/' | docker exec -i kafka kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" --producer.config "$SSL_CONF" --topic test-dual-write-sync >/dev/null
    COUNT=$(docker exec kafka kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config "$SSL_CONF" \
      --topic test-dual-write-sync --group group-dual-validation --from-beginning --max-messages 5 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$COUNT" -eq 5 ]] && pass "Production/Consommation mTLS validée (5/5 messages lus)" || fail "Lecture incomplète ($COUNT/5)"
    ;;

  phase3)
    echo "=== VALIDATION PHASE 3 (Mode KRaft Pur - Sans ZooKeeper) ==="
    if docker ps -a --format '{{.Names}}' | grep -q "^zookeeper$"; then
      fail "Le conteneur zookeeper est encore présent"
    else
      pass "ZooKeeper totalement supprimé"
    fi

    QUORUM=$(docker exec kafka-controller kafka-metadata-quorum --bootstrap-controller "$CONTROLLER" --command-config "$SSL_CONF" describe --status)
    echo "$QUORUM" | grep -q "LeaderId:.*101" && echo "$QUORUM" | grep -q '"id": 1' \
      && pass "Quorum KRaft autonome sain (LeaderId: 101, Observer: 1)" || fail "Quorum KRaft dégradé"

    TOPICS=$(docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" --list)
    echo "$TOPICS" | grep -q "baseline-zk-topic" \
      && pass "Topics historiques conservés après suppression de ZooKeeper" || fail "Topics historiques manquants"

    HIST_COUNT=$(docker exec kafka kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config "$SSL_CONF" \
      --topic baseline-zk-topic --from-beginning --max-messages 10 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$HIST_COUNT" -eq 10 ]] && pass "Données historiques intactes (10/10 messages relus)" || fail "Perte de messages historiques ($HIST_COUNT/10)"

    docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic post-migration-kraft-topic --partitions 1 --replication-factor 1 >/dev/null \
      && pass "Création de nouveau topic en mode KRaft pur réussie" || fail "Échec création topic en mode KRaft"
    ;;

  cleanup)
    echo "=== NETTOYAGE DES TOPICS DE TEST ==="
    for t in baseline-zk-topic test-dual-write-sync post-migration-kraft-topic; do
      docker exec kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
        --delete --if-exists --topic "$t" >/dev/null && echo "Supprimé : $t"
    done
    pass "Nettoyage terminé"
    ;;

  *)
    echo "Phase inconnue : $PHASE (attendu : phase0, phase1, phase2, phase3 ou cleanup)"
    exit 1
    ;;
esac
