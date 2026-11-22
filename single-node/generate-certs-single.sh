#!/bin/bash
set -e

# --- PARAMÈTRES LOCAUX ---
OUTPUT_DIR="./certs"
PASSWORD="changeit"
VALIDITY_DAYS=365
ADVERTISED_HOST="localhost"
# -------------------------

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo "1. Génération de la CA locale..."
openssl req -new -x509 -keyout ca.key -out ca.crt -days $VALIDITY_DAYS -nodes \
  -subj "/CN=Kafka-Local-CA/O=KafkaLocal/C=FR" 2>/dev/null

generate_component_certs() {
  local NAME=$1
  local CN=$2
  local SANS=$3

  cat > "${NAME}-ext.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = ${CN}

[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = ${SANS}
EOF

  openssl req -new -newkey rsa:2048 -nodes \
    -keyout "${NAME}.key" -out "${NAME}.csr" -config "${NAME}-ext.cnf" 2>/dev/null

  openssl x509 -req -in "${NAME}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${NAME}.crt" -days $VALIDITY_DAYS -extensions v3_req -extfile "${NAME}-ext.cnf" 2>/dev/null

  openssl pkcs12 -export -in "${NAME}.crt" -inkey "${NAME}.key" -chain -CAfile ca.crt \
    -name "$NAME" -out "${NAME}.p12" -password pass:"$PASSWORD"

  rm -f "kafka.${NAME}.keystore.jks"
  keytool -importkeystore -deststorepass "$PASSWORD" -destkeypass "$PASSWORD" \
    -destkeystore "kafka.${NAME}.keystore.jks" -srckeystore "${NAME}.p12" \
    -srcstoretype PKCS12 -srcstorepass "$PASSWORD" -alias "$NAME" -noprompt 2>/dev/null

  rm -f "kafka.${NAME}.truststore.jks"
  keytool -keystore "kafka.${NAME}.truststore.jks" -alias CARoot -import \
    -file ca.crt -storepass "$PASSWORD" -noprompt 2>/dev/null

  rm -f "${NAME}.csr" "${NAME}.p12" "${NAME}-ext.cnf"
}

echo "2. Génération des keystores/truststores (Broker, Controller, Client)..."
generate_component_certs "broker" "$ADVERTISED_HOST" "DNS:kafka,DNS:localhost,IP:127.0.0.1"
generate_component_certs "controller" "kafka-controller" "DNS:kafka-controller,DNS:localhost,IP:127.0.0.1"
generate_component_certs "client" "kafka-admin" "DNS:localhost,IP:127.0.0.1"

echo "3. Création des credentials..."
echo "$PASSWORD" > keystore_creds
echo "$PASSWORD" > key_creds
echo "$PASSWORD" > truststore_creds

cat > client-ssl.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ssl.truststore.password=${PASSWORD}
ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
ssl.keystore.password=${PASSWORD}
ssl.key.password=${PASSWORD}
EOF

chmod 644 *.jks *_creds client-ssl.properties ca.crt
echo "Certificats générés dans $(pwd)"