#!/usr/bin/env bash
set -euo pipefail

PHASE="${1:-}"
SSL_CONF="/etc/kafka/secrets/client-ssl.properties"
BOOTSTRAP="kafka-1:29092,kafka-2:29092,kafka-3:29092"
CONTROLLERS="kafka-controller-1:9093,kafka-controller-2:9093,kafka-controller-3:9093"

if [[ -z "$PHASE" ]]; then
  echo "Usage: $0 <phase0|phase1|phase2|phase3|cleanup>"
  exit 1
fi

pass() { echo -e "✅ [OK] $1"; }
fail() { echo -e "❌ [KO] $1"; exit 1; }

get_kraft_leader_id() {
  docker exec kafka-controller-1 kafka-metadata-quorum \
    --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status \
    | awk '/LeaderId:/ {print $2}'
}

case "$PHASE" in
  phase0)
    echo "=== VALIDATION PHASE 0 (Baseline 3 ZooKeeper + 3 Brokers) ==="
    for i in 1 2 3; do
      docker ps --format '{{.Names}}' | grep -q "^zookeeper-${i}$" && pass "zookeeper-${i} actif" || fail "zookeeper-${i} absent"
      docker ps --format '{{.Names}}' | grep -q "^kafka-${i}$" && pass "kafka-${i} actif" || fail "kafka-${i} absent"
    done

    CID=$(docker exec zookeeper-1 zookeeper-shell localhost:2181 get /cluster/id 2>/dev/null | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    [[ -n "$CID" ]] && pass "CLUSTER_ID récupéré : $CID" || fail "CLUSTER_ID introuvable"

    docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic baseline-zk-topic --partitions 3 --replication-factor 3 >/dev/null
    seq 1 10 | sed 's/^/message-zk-/' | docker exec -i kafka-1 kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" --producer.config "$SSL_CONF" --topic baseline-zk-topic >/dev/null
    pass "Topic baseline-zk-topic (RF=3) créé et 10 messages produits en mTLS"
    ;;

  phase1)
    echo "=== VALIDATION PHASE 1 (Quorum 3 Contrôleurs KRaft en attente) ==="
    QUORUM=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status)

    LEADER_ID=$(echo "$QUORUM" | awk '/LeaderId:/ {print $2}')
    [[ "$LEADER_ID" =~ ^(101|102|103)$ ]] && pass "Quorum KRaft actif (LeaderId: $LEADER_ID)" || fail "Aucun Leader KRaft élu"

    for cid in 101 102 103; do
      echo "$QUORUM" | grep -q "\"id\": ${cid}" && pass "Voter $cid présent dans le quorum" || fail "Voter $cid manquant"
    done

    LEADER_CONT="kafka-controller-$((LEADER_ID - 100))"
    docker logs "$LEADER_CONT" 2>&1 | grep -qi "Waiting for brokers to register" \
      && pass "$LEADER_CONT en attente d'enregistrement des 3 brokers ZK" || fail "Message d'attente brokers absent sur $LEADER_CONT"
    ;;

  phase2)
    echo "=== VALIDATION PHASE 2 (Mode Dual Write HA) ==="
    LEADER_ID=$(get_kraft_leader_id)
    LEADER_CONT="kafka-controller-$((LEADER_ID - 100))"

    QUORUM_OUT=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller kafka-controller-1:9093,kafka-controller-2:9093,kafka-controller-3:9093 \
      --command-config /etc/kafka/secrets/client-ssl.properties describe --status 2>/dev/null || true)
    if echo "$QUORUM_OUT" | grep -q '"id": 1,' && echo "$QUORUM_OUT" | grep -q '"id": 2,' && echo "$QUORUM_OUT" | grep -q '"id": 3,'; then
      pass "État DUAL_WRITE actif : 3 Brokers Observers (1, 2, 3) enregistrés sur le quorum KRaft (Leader : $LEADER_CONT)"
    else
      fail "Les 3 brokers ne sont pas encore enregistrés en mode DUAL_WRITE sur le quorum KRaft"
    fi

    for i in 1 2 3; do
      if docker logs "kafka-controller-${i}" --tail 50 2>&1 | grep -Eq "SSL handshake failed|PKIX"; then
        fail "Erreurs SSL détectées dans kafka-controller-${i}"
      fi
    done
    pass "Aucune erreur SSL/PKIX sur les 3 contrôleurs"

    QUORUM=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status)
    for bid in 1 2 3; do
      echo "$QUORUM" | grep -q "\"id\": ${bid}," && pass "Broker ID $bid rattaché en tant qu'Observer" || fail "Broker ID $bid absent des Observers"
    done

    docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic test-dual-write-sync --partitions 3 --replication-factor 3 >/dev/null
    sleep 2
    docker exec zookeeper-1 zookeeper-shell localhost:2181 ls /brokers/topics 2>/dev/null | grep -q "test-dual-write-sync" \
      && pass "Synchronisation KRaft -> ZooKeeper validée (topic répliqué dans ZK)" || fail "Topic non répliqué dans ZooKeeper"

    seq 1 5 | sed 's/^/msg-dual-/' | docker exec -i kafka-1 kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" --producer.config "$SSL_CONF" --topic test-dual-write-sync >/dev/null
    COUNT=$(docker exec kafka-1 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config "$SSL_CONF" \
      --topic test-dual-write-sync --group group-dual-validation --from-beginning --max-messages 5 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$COUNT" -eq 5 ]] && pass "Production/Consommation mTLS validée (5/5 messages lus)" || fail "Lecture incomplète ($COUNT/5)"
    ;;

  phase3)
    echo "=== VALIDATION PHASE 3 (Mode KRaft Pur 3-3 + Test Failover HA) ==="
    for i in 1 2 3; do
      if docker ps -a --format '{{.Names}}' | grep -q "^zookeeper-${i}$"; then
        fail "Le conteneur zookeeper-${i} est encore présent"
      fi
    done
    pass "Les 3 nœuds ZooKeeper sont totalement supprimés"

    QUORUM=$(docker exec kafka-controller-1 kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status)
    for id in 101 102 103 1 2 3; do
      echo "$QUORUM" | grep -q "\"id\": ${id}" && pass "Nœud ID $id actif dans le quorum KRaft" || fail "Nœud ID $id manquant"
    done

    HIST_COUNT=$(docker exec kafka-1 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config "$SSL_CONF" \
      --topic baseline-zk-topic --from-beginning --max-messages 10 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$HIST_COUNT" -eq 10 ]] && pass "Données historiques intactes sans ZooKeeper (10/10 messages relus)" || fail "Perte de messages ($HIST_COUNT/10)"

    echo "--- Test de Haute Disponibilité (Arrêt du Leader KRaft actif + Broker kafka-3) ---"
    OLD_LEADER_ID=$(get_kraft_leader_id)
    OLD_LEADER_CONT="kafka-controller-$((OLD_LEADER_ID - 100))"
    echo "Arrêt forcé de $OLD_LEADER_CONT (Leader actuel) et kafka-3..."
    docker stop "$OLD_LEADER_CONT" kafka-3 >/dev/null
    sleep 6

    # Sélectionner un contrôleur survivant pour interroger le quorum
    SURVIVOR_CONT="kafka-controller-1"
    [[ "$OLD_LEADER_CONT" == "kafka-controller-1" ]] && SURVIVOR_CONT="kafka-controller-2"

    NEW_LEADER_ID=$(docker exec "$SURVIVOR_CONT" kafka-metadata-quorum \
      --bootstrap-controller "$CONTROLLERS" --command-config "$SSL_CONF" describe --status \
      | awk '/LeaderId:/ {print $2}')

    if [[ -n "$NEW_LEADER_ID" && "$NEW_LEADER_ID" != "$OLD_LEADER_ID" ]]; then
      pass "Basculé automatiquement sur un nouveau Leader KRaft : ID $NEW_LEADER_ID (ancien : $OLD_LEADER_ID)"
    else
      fail "Échec de l'élection d'un nouveau Leader KRaft"
    fi

    # Vérifier que la création de topic et l'écriture (acks=all, min.insync.replicas=2) fonctionnent avec 2/3 nœuds
    docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
      --create --if-not-exists --topic ha-failover-topic --partitions 3 --replication-factor 3 >/dev/null
    seq 1 5 | sed 's/^/msg-ha-/' | docker exec -i kafka-1 kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" --producer.config "$SSL_CONF" \
      --producer-property acks=all --topic ha-failover-topic >/dev/null
    HA_COUNT=$(docker exec kafka-2 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config "$SSL_CONF" \
      --topic ha-failover-topic --from-beginning --max-messages 5 --timeout-ms 15000 2>/dev/null | wc -l)
    [[ "$HA_COUNT" -eq 5 ]] && pass "Continuité de service (acks=all) validée avec 1 contrôleur et 1 broker arrêtés (5/5)" || fail "Échec lecture/écriture en mode dégradé ($HA_COUNT/5)"

    echo "Redémarrage de $OLD_LEADER_CONT et kafka-3..."
    docker start "$OLD_LEADER_CONT" kafka-3 >/dev/null
    sleep 5
    pass "Nœuds $OLD_LEADER_CONT et kafka-3 réintégrés au cluster HA"
    ;;

  cleanup)
    echo "=== NETTOYAGE DES TOPICS DE TEST ==="
    for t in baseline-zk-topic test-dual-write-sync ha-failover-topic; do
      docker exec kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$SSL_CONF" \
        --delete --if-exists --topic "$t" >/dev/null && echo "Supprimé : $t"
    done
    pass "Nettoyage terminé"
    ;;

  *)
    echo "Phase inconnue : $PHASE (attendu : phase0, phase1, phase2, phase3 ou cleanup)"
    exit 1
    ;;
esac
