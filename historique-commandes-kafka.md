# Historique des commandes — Déploiement et tests Kafka / KRaft

Ce document regroupe et structure les commandes exécutées lors des tests de déploiement Ansible et de nettoyage Docker.

---

## 1. Commandes principales par action

### Nettoyage de l'environnement Docker

Suppression ciblée des conteneurs Kafka / KRaft :

```bash
docker rm -f $(docker ps -a -q -f name=kafka-broker) 2>/dev/null || true
docker rm -f $(docker ps -a -q -f name=kRAFT) 2>/dev/null || true
docker rm -f $(docker ps -a -q -f name=kraft) $(docker ps -a -q -f name=kafka) 2>/dev/null || true
```

Suppression de tous les conteneurs et purge complète (images, réseaux, volumes inutilisés) :

```bash
docker rm -f $(docker ps -a -q)
docker system prune -a --volumes -f
```

---

### Lancement de ZooKeeper (conteneur autonome)

```bash
docker run -d \
  --name zookeeper \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.9.0
```

---

### Préparation et exécution du playbook Ansible

Création du répertoire de templates de monitoring et déplacement dans le dossier du projet :

```bash
mkdir -p roles/monitoring/templates
cd kafka-deploy/
```

Lancement du playbook Ansible avec la configuration locale :

```bash
ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
```

---

### Vérification du topic Kafka (`test-kraft`)

Consommation des messages depuis le début sur le broker `kafka-broker-101` :

```bash
docker exec -it kafka-broker-101 kafka-console-consumer \
  --bootstrap-server 172.17.0.1:9092 \
  --topic test-kraft \
  --from-beginning
```

---

## 2. Historique brut complet (lignes 272 à 302)

```bash
272  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
273  docker rm -f $(docker ps -a -q -f name=kafka-broker) 2>/dev/null || true
274  docker rm -f $(docker ps -a -q -f name=kRAFT) 2>/dev/null || true
275  docker rm -f $(docker ps -a -q -f name=kraft) $(docker ps -a -q -f name=kafka) 2>/dev/null || true
276  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
277  mkdir -p roles/monitoring/templates
278  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
279  kafka_brokers:
280  docker rm -f $(docker ps -a -q)
281  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
282  docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.9.0
283  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
284  docker rm -f $(docker ps -a -q)
285  docker system prune -a --volumes -f
286  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
287  docker system prune -a --volumes -f
288  docker rm -f $(docker ps -a -q)
289  docker system prune -a --volumes -f
290  docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.9.0
291  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
292  docker exec -it kafka-broker-101 kafka-console-consumer --bootstrap-server 172.17.0.1:9092 --topic test-kraft --from-beginning
293  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
294  cd kafka-deploy/
295  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
296  docker rm -f $(docker ps -a -q)
297  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
298  docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.9.0
299  docker rm -f $(docker ps -a -q)
300  docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.9.0
301  ANSIBLE_CONFIG=./ansible.cfg ANSIBLE_STDOUT_CALLBACK=ansible.builtin.default ansible-playbook -i inventory/dev/hosts.yml site.yml -K
302  docker rm -f $(docker ps -a -q)
```
