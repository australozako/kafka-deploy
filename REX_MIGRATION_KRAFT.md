# Retour d'Expérience (REX) : Migration Kafka ZooKeeper vers KRaft (KIP-833) avec mTLS & Kafbat UI

- **Date :** Novembre 2026
- **Environnement :** Docker Compose sous WSL2, orchestré par Ansible
- **Stack technique :** Confluent Platform `7.9.6` (`cp-zookeeper`, `cp-kafka`), Kafbat UI (`latest`)
- **Sécurité :** Chiffrement et authentification mutuelle TLS (mTLS) sur l'ensemble des listeners (`SSL`, `SSL_INTERNAL`, `CONTROLLER`)
- **Statut :** Migration terminée et validée sans perte de données (`CLUSTER_ID` : `g6d7ZpUBTxSUAAjE-FZ0gA`)

---

## 1. Architecture Réseau, Listeners & Mécanique de Nommage Kafka

Dans une architecture KRaft sécurisée en mTLS, la séparation stricte des flux (clients externes, réplication interne des données et trafic de contrôle du quorum) repose sur des **listeners nommés**.

### 1.1 Rôle des paramètres principaux (Contrôleur vs Broker)

| Paramètre | Côté Contrôleur (`id=101`) | Côté Broker (`id=1`) | Rôle technique exact |
| :--- | :--- | :--- | :--- |
| **`KAFKA_LISTENERS`** | `"CONTROLLER://:9093"` | `"SSL://:9092,SSL_INTERNAL://:29092"` | Définit les interfaces réseau et ports sur lesquels le processus **écoute** localement. Le contrôleur n'ouvre que le port de quorum `9093`, tandis que le broker ouvre `9092` (clients) et `29092` (interne). |
| **`KAFKA_ADVERTISED_LISTENERS`** | *Non requis (port dans `QUORUM_VOTERS`)* | `"SSL://localhost:9092,SSL_INTERNAL://kafka:29092"` | Adresses retournées aux clients dans les métadonnées après la première connexion. Permet d'orienter les conteneurs Docker vers le DNS interne `kafka:29092` et l'hôte WSL2 vers `localhost:9092`. |
| **`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`** | `"CONTROLLER:SSL,SSL_INTERNAL:SSL"` | `"SSL:SSL,SSL_INTERNAL:SSL,CONTROLLER:SSL"` | **Table de correspondance** associant chaque *nom logique de listener* (choisi librement par l'administrateur) à un *protocole de sécurité Kafka standard* (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`). Ici, nos trois noms logiques (`SSL`, `SSL_INTERNAL`, `CONTROLLER`) utilisent tous le protocole `SSL`. |
| **`KAFKA_CONTROLLER_LISTENER_NAMES`** | `"CONTROLLER"` | `"CONTROLLER"` | Indique à Kafka quel listener de la *protocol map* est réservé aux communications du **quorum KRaft** (élections Raft, réplication de `__cluster_metadata` et enregistrement des brokers). |
| **`KAFKA_INTER_BROKER_LISTENER_NAME`** | `"SSL_INTERNAL"` | `"SSL_INTERNAL"` | Définit le listener utilisé pour le trafic interne entre nœuds. **Crucial pendant la migration KRaft :** c'est via `SSL_INTERNAL` (`kafka:29092`) que le contrôleur KRaft établit sa connexion sortante (`ControllerChannelManager`) vers le broker ZooKeeper pour lui pousser les mises à jour de métadonnées en Phase 1 et Phase 2. |

### 1.2 Pourquoi ce nommage `KAFKA_LISTENER_NAME_<NOM>_*` ?

Confluent Platform convertit automatiquement les variables d'environnement Docker en propriétés Java dans `/etc/kafka/kafka.properties` selon la règle suivante :
1. Suppression du préfixe `KAFKA_`.
2. Passage en minuscules.
3. Remplacement des underscores `_` par des points `.`.

D'après la spécification Apache Kafka, pour configurer le chiffrement d'un listener nommé `<LISTENER_NAME>` (en minuscules), la propriété attendue est :
`listener.name.<listener_name>.ssl.<propriété>`

Par conséquent :
- Pour le listener nommé **`CONTROLLER`**, la propriété `listener.name.controller.ssl.keystore.location` s'écrit en variable d'environnement :
  `KAFKA_LISTENER_NAME_CONTROLLER_SSL_KEYSTORE_LOCATION`
- Pour le listener nommé **`SSL_INTERNAL`**, le nom logique contient déjà un underscore (`ssl_internal`), ce qui donne `listener.name.ssl_internal.ssl.keystore.location`, soit en variable d'environnement :
  `KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_KEYSTORE_LOCATION`

> **Pourquoi déclarer `KAFKA_LISTENER_NAME_SSL_INTERNAL_*` sur le contrôleur alors qu'il n'écoute pas sur `SSL_INTERNAL` ?**
> Parce que dans Kafka, lorsqu'un nœud (ici le contrôleur) initie une connexion **cliente sortante** vers un listener nommé `SSL_INTERNAL` sur un broker, son client réseau interne (`Selector` / `SslFactory`) recherche en priorité les certificats préfixés par `listener.name.ssl_internal.ssl.*`, puis se rabat sur les propriétés globales `ssl.*`.

---

## 2. Catalogue Complet des 6 Incidents Rencontrés & Résolutions

### Incident 1 : Échec d'authentification mTLS du contrôleur vers le broker (`RequestSendThread` / `SSL_INTERNAL`)
- **Symptôme :** En Phase 1 et Phase 2, le contrôleur KRaft démarre mais reste bloqué avec des erreurs `SSL handshake failed` dans les logs de `RequestSendThread` / `ControllerChannelManager` lorsqu'il tente de joindre `kafka:29092` (`SSL_INTERNAL`), empêchant la transition vers `DUAL_WRITE`.
- **Cause racine :** Les images Confluent utilisent habituellement des fichiers de secrets via `KAFKA_SSL_KEYSTORE_FILENAME` et `KAFKA_SSL_KEYSTORE_CREDENTIALS`. Cependant, les composants internes clients du contrôleur KRaft (`RequestSendThread`) et certains outils d'administration ignorent ces variables de commodité Confluent et exigent les propriétés Java standard `ssl.keystore.location`, `ssl.truststore.location` et leurs mots de passe explicites dans `/etc/kafka/kafka.properties`.
- **Résolution :** Ajout explicite des variables SSL globales **et** des variables préfixées `KAFKA_LISTENER_NAME_SSL_INTERNAL_*` sur `kafka-controller` :

```yaml
KAFKA_SSL_KEYSTORE_LOCATION: "/etc/kafka/secrets/{{ controller_keystore }}"
KAFKA_SSL_KEYSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_SSL_KEY_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_SSL_TRUSTSTORE_LOCATION: "/etc/kafka/secrets/{{ controller_truststore }}"
KAFKA_SSL_TRUSTSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"

KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_KEYSTORE_LOCATION: "/etc/kafka/secrets/{{ controller_keystore }}"
KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_KEYSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_KEY_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_TRUSTSTORE_LOCATION: "/etc/kafka/secrets/{{ controller_truststore }}"
KAFKA_LISTENER_NAME_SSL_INTERNAL_SSL_TRUSTSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"
```
Incident 2 : Absence de configuration SSL du listener CONTROLLER côté Broker en Phase 2 & 3
Symptôme : Lors du passage en Phase 2 (Dual Write), le broker ZooKeeper (id=1) échoue à s'enregistrer auprès du contrôleur (kafka-controller:9093) avec une erreur de handshake SSL ou de protocole manquant.

Cause racine : En Phase 0 et 1, le broker ne connaissait que SSL et SSL_INTERNAL. En Phase 2 et 3, il devient client du quorum KRaft sur le listener CONTROLLER : il doit donc non seulement déclarer CONTROLLER:SSL dans KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, mais aussi fournir son propre keystore/truststore via KAFKA_LISTENER_NAME_CONTROLLER_SSL_* pour s'authentifier auprès du contrôleur (KAFKA_SSL_CLIENT_AUTH: "required").

Résolution : Injection conditionnelle des variables KAFKA_LISTENER_NAME_CONTROLLER_SSL_* dans templates/docker-compose.yml.j2 pour migration_phase in ['phase2', 'phase3'] :

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "SSL:SSL,SSL_INTERNAL:SSL,CONTROLLER:SSL"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_KEYSTORE_LOCATION: "/etc/kafka/secrets/{{ broker_keystore }}"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_KEYSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_KEY_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_TRUSTSTORE_LOCATION: "/etc/kafka/secrets/{{ broker_truststore }}"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_TRUSTSTORE_PASSWORD: "{{ ssl_password | default('changeit') }}"
KAFKA_LISTENER_NAME_CONTROLLER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: ""
```

Incident 3 : Rejet du certificat par validation DNS (No subject alternative DNS name matching kafka-controller found)
Symptôme : Les commandes CLI d'administration (kafka-metadata-quorum --bootstrap-controller localhost:9093 describe --status) échouent avec :javax.net.ssl.SSLHandshakeException: No subject alternative DNS name matching kafka-controller found.

Cause racine : Même lorsque la connexion initiale cible localhost:9093, le protocole Kafka retourne l'endpoint annoncé du leader (kafka-controller:9093). Par défaut depuis Kafka 2.0+, le client Java active la vérification stricte du nom d'hôte (ssl.endpoint.identification.algorithm=HTTPS), qui rejette le certificat si kafka-controller ne figure pas dans les Subject Alternative Names (SAN) du certificat X.509.

Résolution :

Côté conteneurs : KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: "" (global et par listener).

Côté CLI d'administration (certs/client-ssl.properties) : ajout de ssl.endpoint.identification.algorithm=.

Incident 4 : Erreur de connexion mTLS de Kafbat UI après bascule SSL interne
Symptôme : Kafbat UI (http://localhost:8080) reste en statut Offline ou échoue au healthcheck Ansible (/actuator/health).

Cause racine : Kafbat UI interroge le broker sur le réseau interne Docker (kafka:29092), qui exige désormais l'authentification mutuelle par certificat client (KAFKA_SSL_CLIENT_AUTH: "required"). De plus, en Phase 3, le maintien de la variable KAFKA_CLUSTERS_0_ZOOKEEPER fait pointer Kafbat UI vers un nœud supprimé.

Résolution : Montage du volume /etc/kafka/secrets:ro dans le conteneur kafbat-ui, configuration des propriétés KAFKA_CLUSTERS_0_PROPERTIES_SSL_KEYSTORE_* et TRUSTSTORE_*, et retrait conditionnel de KAFKA_CLUSTERS_0_ZOOKEEPER en Phase 3.

Incident 5 : Erreurs de syntaxe YAML (go-yaml) dans templates/docker-compose.yml.j2
Symptôme : Échec de docker compose up lors de la génération de docker-compose.yml avec des erreurs yaml: line X: did not find expected key.

Cause racine : Décalages d'indentation introduits autour des balises Jinja2 {% if migration_phase ... %} (listes - sous ports:/volumes: alignées à 4 espaces au lieu de 6, clé kafka-controller: à colonne 0, et variables KAFKA_ZOOKEEPER_* à 5 espaces au lieu de 6).

Résolution : Alignement strict à 2 espaces par niveau hiérarchique YAML (services à 2 espaces, propriétés à 4 espaces, variables environment: et items de listes - à 6 espaces) et validation systématique avec docker compose config.

Incident 6 : Idempotence Ansible et persistance du CLUSTER_ID en Phase 2 & 3
Symptôme :

En Phase 2, la vérification grep 'Completed migration of metadata from ZooKeeper to KRaft' échouait lors d'un redémarrage du contrôleur, car les métadonnées étaient déjà migrées et le driver passait directement à to DUAL_WRITE state.

En Phase 3, docker compose up -d --remove-orphans supprimant zookeeper, une ré-exécution du playbook ne pouvait plus lire /cluster/id depuis ZooKeeper.

Résolution :

Sauvegarde automatique de detected_cluster_id dans un fichier local .cluster_id dès la Phase 1/2, lu en priorité par Ansible avant toute interrogation de ZooKeeper.

Élargissement de la regex de contrôle Phase 2 dans playbook.yml (grep -E 'Completed migration of metadata from ZooKeeper to KRaft|to DUAL_WRITE state').

3. Résultats de Validation & Liens Officiels
Synthèse des contrôles validés (./validate-migration.sh phase3)
ZooKeeper totalement supprimé : Seuls kafka-controller, kafka et kafbat-ui sont actifs.

Quorum KRaft autonome sain : ClusterId: g6d7ZpUBTxSUAAjE-FZ0gA, LeaderId: 101, CurrentVoters: [101], CurrentObservers: [1], MaxFollowerLag: 0.

Intégrité des données : 100 % des topics et messages créés avant la migration (baseline-zk-topic) relus sans erreur, et création opérationnelle de nouveaux topics en mode KRaft pur.

Documentation Officielle de Référence
Confluent Platform — ZooKeeper to KRaft Migration (Procédure officielle & Dual Write) : https://docs.confluent.io/platform/current/installation/migrate-zk-kraft.html

Confluent Platform — Configuration SSL / mTLS par Listener (listener.name.<name>.ssl.*) : https://docs.confluent.io/platform/current/kafka/authentication_ssl.html

Confluent Platform — Règles de conversion des variables d'environnement Docker (KAFKA_*) : https://docs.confluent.io/platform/current/installation/docker/config-reference.html

Apache Kafka — KIP-833 : Mark KRaft as Production Ready & ZooKeeper Migration : https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A+Mark+KRaft+as+Production+Ready

Apache Kafka — Configuration des Brokers & Contrôleurs KRaft (controller.listener.names, inter.broker.listener.name) : https://kafka.apache.org/documentation/#kraft