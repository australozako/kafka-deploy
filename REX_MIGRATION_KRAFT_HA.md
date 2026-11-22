# Retour d'Expérience (REX) : Industrialisation d'une Migration Kafka ZooKeeper vers KRaft HA (3-3-3) sans Interruption (Rolling Restart & Live Monitoring)

* **Date :** Novembre 2026
* **Environnement :** Docker Compose sous WSL2, orchestré par Ansible & Bash
* **Stack technique :** Confluent Platform `7.9.6` (`cp-zookeeper`, `cp-kafka`), Kafbat UI (`latest`)
* **Topologie :** Cluster Haute Disponibilité **3 ZooKeeper $\rightarrow$ 3 Brokers + 3 Contrôleurs KRaft**
* **Sécurité :** PKI interne automatisée (`generate-certs.sh`) avec chiffrement et authentification mutuelle TLS (mTLS) sur tous les listeners (`SSL`, `SSL_INTERNAL`, `CONTROLLER`)
* **Statut :** Migration complète sans interruption de service (`zero-downtime`), supervisée en temps réel et validée sous panne simultanée (Failover HA)

---

## 1. Architecture Réseau, Topologie HA & Mécanique de Nommage Kafka

### 1.1 Topologie du Cluster 3-3-3 & Séparation des Plages d'IDs

Pour éliminer tout risque de collision d'identifiants dans le journal interne `__cluster_metadata` (KIP-833), les contrôleurs et les brokers utilisent des plages de `NODE_ID` distinctes :

| Rôle | Conteneurs | IDs (`NODE_ID` / `BROKER_ID`) | Ports externes (Hôte) | Ports internes (Docker) |
| :--- | :--- | :--- | :--- | :--- |
| **Ensemble ZooKeeper** *(Phases 0–2)* | `zookeeper-1`, `zookeeper-2`, `zookeeper-3` | `1`, `2`, `3` | `2181`, `2182`, `2183` | `2181` (client), `2888` (peer), `3888` (election) |
| **Quorum KRaft** *(Phases 1–3)* | `kafka-controller-1`, `kafka-controller-2`, `kafka-controller-3` | `101`, `102`, `103` | `9093`, `9094`, `9095` | `9093` (`CONTROLLER`) |
| **Brokers Kafka** *(Phases 0–3)* | `kafka-1`, `kafka-2`, `kafka-3` | `1`, `2`, `3` | `9092`, `9192`, `9292` | `9092` (`SSL`), `29092` (`SSL_INTERNAL`) |
| **Supervision** | `kafbat-ui` | — | `8080` | `8080` |

### 1.2 Rôle des paramètres de Listeners (Contrôleur vs Broker)

| Paramètre | Côté Contrôleurs (`101..103`) | Côté Brokers (`1..3`) | Rôle technique exact |
| :--- | :--- | :--- | :--- |
| **`KAFKA_LISTENERS`** | `"CONTROLLER://:9093"` | `"SSL://:9092,SSL_INTERNAL://:29092"` | Interfaces réseau et ports d'écoute locaux. Les contrôleurs n'exposent que le port de quorum `9093`. |
| **`KAFKA_ADVERTISED_LISTENERS`** | *Géré via `QUORUM_VOTERS`* | `"SSL://localhost:9092..9292,SSL_INTERNAL://kafka-X:29092"` | Adresses retournées dans les métadonnées : DNS interne `kafka-X:29092` pour le trafic inter-brokers/Kafbat UI, et ports mappés `localhost` pour l'hôte. |
| **`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`** | `"CONTROLLER:SSL,SSL_INTERNAL:SSL"` | `"SSL:SSL,SSL_INTERNAL:SSL,CONTROLLER:SSL"` | Associe chaque **nom logique de listener** au protocole de sécurité Kafka `SSL`. |
| **`KAFKA_CONTROLLER_LISTENER_NAMES`** | `"CONTROLLER"` | `"CONTROLLER"` | Désigne le listener dédié aux élections Raft, à la réplication de `__cluster_metadata` et à l'enregistrement des brokers. |
| **`KAFKA_INTER_BROKER_LISTENER_NAME`** | `"SSL_INTERNAL"` | `"SSL_INTERNAL"` | Définit le canal interne entre nœuds. Pendant les Phases 1 et 2, le **Leader KRaft** utilise `SSL_INTERNAL` (`kafka-X:29092`) pour pousser les mises à jour de métadonnées aux brokers encore rattachés à ZooKeeper. |

### 1.3 Mécanique de nommage `KAFKA_LISTENER_NAME_<NOM>_*`

Confluent Platform traduit les variables d'environnement en propriétés Java `/etc/kafka/kafka.properties` en retirant `KAFKA_`, en passant en minuscules et en remplaçant `_` par `.` :
* `KAFKA_LISTENER_NAME_CONTROLLER_SSL_KEYSTORE_LOCATION` $\rightarrow$ `listener.name.controller.ssl.keystore.location`
* `KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_KEYSTORE_LOCATION` $\rightarrow$ `listener.name.ssl_internal.ssl.keystore.location`

> **Pourquoi configurer `KAFKA_LISTENER_NAME_SSL_INTERNAL_*` sur les contrôleurs ?**
> Lorsque le contrôleur Leader ouvre une connexion **cliente sortante** vers le listener nommé `SSL_INTERNAL` d'un broker, sa fabrique SSL interne (`SslFactory`) recherche en priorité les certificats préfixés par `listener.name.ssl_internal.ssl.*`, avant de se rabattre sur les propriétés globales `ssl.*`.

---

## 2. Outillage d'Industrialisation, Rolling Restart & Supervision Live

### 2.1 Déploiement & Validation
* **`generate-certs.sh` :** Génère de façon idempotente la CA racine (`ca.crt`), le truststore partagé (`kafka.truststore.jks`), les 3 keystores brokers (`kafka-1..3.keystore.jks`), les 3 keystores contrôleurs (`kafka-controller-1..3.keystore.jks`) avec SAN DNS/IP et usages `serverAuth,clientAuth`, ainsi que le keystore client/Kafbat UI et les fichiers de credentials Confluent.
* **`templates/docker-compose.yml.j2` :** Génère dynamiquement les 9 nœuds via boucles Jinja2 selon `migration_phase` (`phase0` à `phase3`), avec paramètres de haute disponibilité (`DEFAULT_REPLICATION_FACTOR=3`, `MIN_INSYNC_REPLICAS=2`, `OFFSETS_TOPIC_REPLICATION_FACTOR=3`).
* **`playbook.yml` :** Orchestre la migration, met en cache le `CLUSTER_ID` dans `.cluster_id`, interroge le quorum KRaft pour **détecter dynamiquement le Leader actif** et valide l'enregistrement des 3 Voters et 3 Observers.
* **`validate-migration.sh` & `reset-and-run.sh` :** Permettent de détruire intégralement les 9 conteneurs / 12 volumes (`destroy`) et de rejouer le cycle complet (`full-cycle`) incluant un test de failover réel.

### 2.2 Bascule sans Interruption (`rolling-restart.sh`)
Pour éviter toute coupure de service lors des transitions `phase1` $\rightarrow$ `phase2` et `phase2` $\rightarrow$ `phase3`, `rolling-restart.sh` applique strictement l'ordre recommandé par KIP-833 :
1. **Rendu du manifeste cible :** Génère `docker-compose.yml` pour la phase cible (`phase2` ou `phase3`) avec chargement explicite de `@group_vars/all.yml` et du `CLUSTER_ID` mis en cache, sans déclencher d'arrêt massif.
2. **Rolling Restart des Contrôleurs (en Phase 3) :** Met à jour et redémarre séquentiellement `kafka-controller-1` $\rightarrow$ `kafka-controller-2` $\rightarrow$ `kafka-controller-3` (retrait de `KAFKA_ZOOKEEPER_METADATA_MIGRATION_ENABLE`), en attendant après chaque nœud qu'un Leader Raft valide soit élu (`LeaderId` $\in \{101, 102, 103\}$).
3. **Rolling Restart des Brokers :** Met à jour et redémarre séquentiellement `kafka-1` $\rightarrow$ `kafka-2` $\rightarrow$ `kafka-3` via `docker compose up -d --no-deps`, en attendant après chaque redémarrage que toutes les partitions aient rattrapé leur ISR (`UnderReplicatedPartitions == 0`).
4. **Convergence post-migration :** Vérifie activement l'enregistrement des 3 brokers (`id: 1, 2, 3`) dans `CurrentObservers` du quorum KRaft avant de clôturer la transition.

### 2.3 Supervision Canary Temps Réel (`live-monitor.sh`)
Pendant l'exécution de `rolling-restart.sh`, `live-monitor.sh` valide en continu la disponibilité du cluster :
* **Flux Canary `acks=all` :** Un producteur injecte un message horodaté toutes les `0,5 s` dans le topic `live-migration-canary` (`RF=3`, `min.insync.replicas=2`) tandis qu'un consommateur temps réel vérifie la réception continue et comptabilise les erreurs critiques (`ERROR` / `Exception`).
* **Observabilité des métriques critiques :** Affiche toutes les 2 secondes l'état des 9 conteneurs, l'identité du `LeaderId` KRaft, le lag maximal des followers (`MaxFollowerLag`) et le compteur exact de partitions sous-répliquées (`grep -c "Partition:"`).

---

## 3. Catalogue Complet des 10 Incidents Rencontrés & Résolutions

### Incident 1 : Échec du handshake mTLS sortant des contrôleurs (`RequestSendThread` / `SSL_INTERNAL`)
* **Symptôme :** En Phase 1/2, les contrôleurs échouaient avec `SSL handshake failed` dans `RequestSendThread` en tentant de joindre `kafka-X:29092`.
* **Cause racine :** Les composants clients internes du contrôleur KRaft ignorent les variables de commodité Confluent (`KAFKA_SSL_KEYSTORE_FILENAME`) et exigent les propriétés Java explicites `ssl.keystore.location`, `ssl.truststore.location` et leurs mots de passe.
* **Résolution :** Déclaration explicite des variables SSL globales **et** des variables `KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_*` sur chaque contrôleur.

### Incident 2 : Absence de configuration SSL du listener `CONTROLLER` côté Brokers en Phase 2 & 3
* **Symptôme :** Au passage en Phase 2, les brokers (`id=1..3`) échouaient à s'enregistrer auprès du quorum KRaft (`9093`).
* **Cause racine :** En devenant clients du quorum KRaft sur le listener `CONTROLLER` avec `KAFKA_SSL_CLIENT_AUTH: "required"`, chaque broker doit présenter son propre certificat via `KAFKA_LISTENER_NAME_CONTROLLER_SSL_*`.
* **Résolution :** Injection conditionnelle des variables `KAFKA_LISTENER_NAME_CONTROLLER_SSL_*` sur `kafka-1..3` pour `migration_phase in ['phase2', 'phase3']`.

### Incident 3 : Rejet de certificat par validation DNS (`No subject alternative DNS name`)
* **Symptôme :** Les commandes CLI (`kafka-metadata-quorum`) échouaient avec `SSLHandshakeException: No subject alternative DNS name matching kafka-controller-X found`.
* **Cause racine :** Après connexion initiale, Kafka redirige le client vers l'adresse annoncée du Leader (`kafka-controller-X:9093`), déclenchant la vérification stricte HTTPS du SAN.
* **Résolution :** Génération de certificats dédiés par nœud embarquant les SAN exacts (`DNS:kafka-controller-X,DNS:localhost,IP:127.0.0.1`) dans `generate-certs.sh`, couplée à `ssl.endpoint.identification.algorithm=` dans `client-ssl.properties`.

### Incident 4 : Connexion mTLS de Kafbat UI et référence ZooKeeper orpheline
* **Symptôme :** Kafbat UI restait `Offline` sur le réseau interne mTLS (`kafka-1..3:29092`) et tentait encore de joindre ZooKeeper en Phase 3.
* **Cause racine :** Le listener interne `SSL_INTERNAL` exige un certificat client et la variable `KAFKA_CLUSTERS_0_ZOOKEEPER` n'était pas conditionnée à la phase.
* **Résolution :** Montage de `certs/` en lecture seule sur `kafbat-ui`, configuration de `kafka.client.keystore.jks` et retrait conditionnel de `KAFKA_CLUSTERS_0_ZOOKEEPER` en `phase3`.

### Incident 5 : Erreurs d'indentation YAML (`go-yaml`) dans `templates/docker-compose.yml.j2`
* **Symptôme :** Échec de `docker compose up` (`yaml: did not find expected key`).
* **Cause racine :** Décalages d'espaces autour des blocs Jinja2 `{% if %}` / `{% for %}`.
* **Résolution :** Normalisation stricte à 2 espaces par niveau hiérarchique YAML et suppression de la directive obsolète `version:`.

### Incident 6 : Idempotence Ansible et persistance du `CLUSTER_ID`
* **Symptôme :** Échec de récupération du `/cluster/id` après suppression de ZooKeeper en Phase 3.
* **Cause racine :** `docker compose up --remove-orphans` supprime `zookeeper-1..3` en Phase 3.
* **Résolution :** Mise en cache automatique du `CLUSTER_ID` dans `.cluster_id` dès la Phase 1/2.

### Incident 7 : Absence de `netcat` (`nc`) dans l'image `cp-zookeeper:7.9.6`
* **Symptôme :** La vérification Phase 0 `echo ruok | nc localhost 2181` échouait avec `rc=1`.
* **Cause racine :** L'utilitaire `nc` n'est pas embarqué dans `confluentinc/cp-zookeeper:7.9.6` et les *4-letter words* sont bridés par défaut.
* **Résolution :** Remplacement par la commande native `zookeeper-shell localhost:2181 ls /`.

### Incident 8 : Détection du Leader KRaft dynamique et délai d'initialisation de `__consumer_offsets`
* **Symptôme :** Dans le cluster à 3 contrôleurs, le contrôleur qui exécute le driver `DUAL_WRITE` n'est pas fixe (`101`, `102` ou `103`), et le premier `kafka-console-consumer` expirait à `5000 ms` lors de la création à froid de `__consumer_offsets` (`RF=3`).
* **Cause racine :** Élection dynamique du Leader Raft en multi-contrôleurs et latence initiale du *Group Coordinator*.
* **Résolution :** Interrogation dynamique de `LeaderId` via `kafka-metadata-quorum`, vérification de la présence des 3 brokers dans `CurrentObservers` et augmentation du `--timeout-ms` de test à `15000`.

### Incident 9 : Ordre du Rolling Restart en Phase 3 (Contrôleurs avant Brokers) et reconnexion des Observers
* **Symptôme :** Redémarrer les brokers avant les contrôleurs lors du passage en Phase 3 entraînait une déconnexion transitoire des `CurrentObservers` au moment où les contrôleurs redémarraient juste après.
* **Cause racine :** En Phase 3, ce sont d'abord les contrôleurs qui doivent abandonner le mode migration (`zookeeper.metadata.migration.enable=false`), puis les brokers qui doivent basculer sur le quorum KRaft pur.
* **Résolution :** Inversion de la séquence dans `rolling-restart.sh` pour la Phase 3 (Rolling Restart des 3 contrôleurs avec attente du Leader Raft, suivi du Rolling Restart des 3 brokers avec attente de `URP = 0`) et boucle d'attente active sur `CurrentObservers`.

### Incident 10 : Comptage précis des partitions sous-répliquées (`URP`) et chargement des `group_vars` hors playbook
* **Symptôme :** `kafka-topics --describe --under-replicated-partitions | wc -l` incluait les lignes d'en-tête `Topic:` dans le décompte, et l'appel direct `ansible -m template` dans `rolling-restart.sh` ne chargeait pas `group_vars/all.yml`.
* **Cause racine :** Format de sortie multi-lignes de `kafka-topics --describe` et portée des variables Ansible en mode ad-hoc.
* **Résolution :** Filtrage strict par `grep -c "Partition:"` et passage explicite de `-e "@group_vars/all.yml"` lors du rendu du template.

---

## 4. Résultats de Validation, Rolling Restart & Test de Failover HA

L'exécution combinée de `live-monitor.sh`, `rolling-restart.sh` et `validate-migration.sh` a validé l'ensemble des exigences de production :

1. **Bascule Zero-Downtime validée :** Pendant le Rolling Restart `phase1` $\rightarrow$ `phase2` puis `phase2` $\rightarrow$ `phase3`, le flux canary (`acks=all`, `RF=3`, `min.insync.replicas=2`) a continué d'être produit et consommé sans interruption ni erreur critique, avec rattrapage automatique des 53 partitions (`50` de `__consumer_offsets` + `3` du canary) à `URP = 0` après chaque redémarrage de nœud.
2. **Intégrité des données historiques :** Les `10/10` messages produits en Phase 0 (`baseline-zk-topic`) sous ZooKeeper ont été relus intégralement en Phase 3 après suppression des 3 conteneurs `zookeeper-1..3`.
3. **Test de Failover HA validé (`validate-migration.sh phase3`) :**
    * Arrêt brutal simultané du **Leader KRaft actif** (`docker stop kafka-controller-X`) et d'un **Broker** (`docker stop kafka-3`).
    * **Réélection automatique** d'un nouveau Leader KRaft parmi les 2 contrôleurs survivants (quorum 2/3) en moins de 6 secondes.
    * **Continuité de service garantie :** Création du topic `ha-failover-topic` (`RF=3`), production de 5 messages avec `acks=all` et consommation intégrale (`5/5`) sur les 2 brokers restants, suivies de la réintégration propre des nœuds redémarrés.

---

## 5. Documentation Officielle de Référence

* **Confluent Platform — ZooKeeper to KRaft Migration (Dual Write & Rolling Restart) :**
  [https://docs.confluent.io/platform/current/installation/migrate-zk-kraft.html](https://docs.confluent.io/platform/current/installation/migrate-zk-kraft.html)
* **Confluent Platform — Configuration SSL / mTLS par Listener (`listener.name.<name>.ssl.*`) :**
  [https://docs.confluent.io/platform/current/kafka/authentication_ssl.html](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html)
* **Confluent Platform — Référence Docker & Conversion des variables `KAFKA_*` :**
  [https://docs.confluent.io/platform/current/installation/docker/config-reference.html](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)
* **Apache Kafka — KIP-833 : Mark KRaft as Production Ready :**
  [https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A+Mark+KRaft+as+Production+Ready](https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A+Mark+KRaft+as+Production+Ready)
* **Apache Kafka — Documentation officielle KRaft (`controller.quorum.voters`, `node.id`, `process.roles`) :**
  [https://kafka.apache.org/documentation/#kraft](https://kafka.apache.org/documentation/#kraft)
