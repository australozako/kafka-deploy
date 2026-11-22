#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/mnt/c/Users/sirib/kafka-deploy"
CERTS_DIR="$PROJECT_DIR/certs"
PASSWORD="${1:-changeit}"
VALIDITY_DAYS=3650

echo "=== GÉNÉRATION DE L'INFRASTRUCTURE mTLS (3 BROKERS + 3 CONTRÔLEURS) ==="

# 1. Préparation propre du répertoire certs/
rm -rf "$CERTS_DIR"
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

# 2. Génération de l'Autorité de Certification (CA)
openssl req -new -x509 -nodes \
  -keyout ca.key \
  -out ca.crt \
  -days "$VALIDITY_DAYS" \
  -subj "/CN=Kafka-Internal-CA/OU=Infra/O=Migration/L=Paris/C=FR" \
  2>/dev/null
echo "✅ [OK] CA racine générée (ca.crt / ca.key)"

# 3. Création du Truststore partagé
keytool -keystore kafka.truststore.jks \
  -alias CARoot \
  -import -file ca.crt \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -noprompt >/dev/null 2>&1
echo "✅ [OK] Truststore partagé généré (kafka.truststore.jks)"

# Fonction générique de création et signature d'un keystore avec SAN
create_signed_keystore() {
  local node_name="$1"
  local san_ext="$2"
  local keystore_file="${node_name}.keystore.jks"

  # Génération de la paire de clés
  keytool -genkey -v \
    -keystore "$keystore_file" \
    -alias "$node_name" \
    -keyalg RSA \
    -keysize 2048 \
    -validity "$VALIDITY_DAYS" \
    -dname "CN=${node_name},OU=Infra,O=Migration,L=Paris,C=FR" \
    -ext "SAN=${san_ext}" \
    -storepass "$PASSWORD" \
    -keypass "$PASSWORD" >/dev/null 2>&1

  # Création de la demande de signature (CSR)
  keytool -certreq \
    -keystore "$keystore_file" \
    -alias "$node_name" \
    -file "${node_name}.csr" \
    -storepass "$PASSWORD" >/dev/null 2>&1

  # Signature par la CA avec conservation explicite des extensions SAN
  cat << EXT > "${node_name}.ext"
subjectAltName = ${san_ext}
extendedKeyUsage = serverAuth,clientAuth
EXT

  openssl x509 -req \
    -CA ca.crt \
    -CAkey ca.key \
    -in "${node_name}.csr" \
    -out "${node_name}.crt" \
    -days "$VALIDITY_DAYS" \
    -CAcreateserial \
    -extfile "${node_name}.ext" \
    2>/dev/null

  # Import de la CA puis du certificat signé dans le keystore
  keytool -keystore "$keystore_file" \
    -alias CARoot \
    -import -file ca.crt \
    -storepass "$PASSWORD" \
    -noprompt >/dev/null 2>&1

  keytool -keystore "$keystore_file" \
    -alias "$node_name" \
    -import -file "${node_name}.crt" \
    -storepass "$PASSWORD" \
    -noprompt >/dev/null 2>&1

  # Nettoyage des fichiers intermédiaires
  rm -f "${node_name}.csr" "${node_name}.crt" "${node_name}.ext"
  echo "✅ [OK] Keystore signé généré : ${keystore_file} (SAN: ${san_ext})"
}

# 4. Génération des 3 Keystores Brokers
for i in 1 2 3; do
  create_signed_keystore "kafka-${i}" "DNS:kafka-${i},DNS:localhost,IP:127.0.0.1"
done

# 5. Génération des 3 Keystores Contrôleurs KRaft
for i in 1 2 3; do
  create_signed_keystore "kafka-controller-${i}" "DNS:kafka-controller-${i},DNS:localhost,IP:127.0.0.1"
done

# 6. Génération du Keystore Client (CLI & Kafbat UI)
create_signed_keystore "kafka.client" "DNS:localhost,DNS:kafbat-ui,IP:127.0.0.1"

# 7. Fichiers de credentials requis par les images Docker Confluent
echo -n "$PASSWORD" > keystore_creds
echo -n "$PASSWORD" > key_creds
echo -n "$PASSWORD" > truststore_creds

# 8. Fichier client-ssl.properties pour les commandes CLI (kafka-topics, kafka-metadata-quorum, etc.)
cat << PROPS > client-ssl.properties
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=${PASSWORD}
ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
ssl.keystore.password=${PASSWORD}
ssl.key.password=${PASSWORD}
ssl.endpoint.identification.algorithm=
PROPS

# 9. Permissions de lecture pour les conteneurs Confluent (appuser UID 1000)
chmod 644 "$CERTS_DIR"/*

echo ""
echo "🎉 Tous les certificats, keystores et fichiers de configuration SSL sont prêts dans $CERTS_DIR"
