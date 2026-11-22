# kafka-deploy
# kafka-deploy
# kafka-deploy
## 3. Scénario de test complet en local

### Étape 1 : Lancer l'existant (Phase 0) et créer un topic de test

```bash
ansible-playbook -i inventory.ini playbook.yml -e "migration_phase=phase0"

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --create \
  --topic test-migration \
  --partitions 1 \
  --replication-factor 1
```

> **Vérification :** Le topic `test-migration` est créé avec succès via SSL.

---

### Étape 2 : Ajouter le contrôleur KRaft (Phase 1)

```bash
ansible-playbook -i inventory.ini playbook.yml -e "migration_phase=phase1"
```

> **Vérification :** Notez le `CLUSTER_ID` affiché dans la sortie Ansible et renseignez-le dans `group_vars/all.yml`.

---

### Étape 3 : Activer le Dual Write (Phase 2)

```bash
ansible-playbook -i inventory.ini playbook.yml -e "migration_phase=phase2"
```

> **Vérification :** Vérifiez que le topic existant est toujours accessible :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --list
```

---

### Étape 4 : Basculer en KRaft pur et supprimer ZooKeeper (Phase 3)

```bash
ansible-playbook -i inventory.ini playbook.yml -e "migration_phase=phase3"
```

> **Vérification :** La commande `docker ps` ne doit plus afficher le conteneur `zookeeper`, et le topic `test-migration` doit toujours être présent et fonctionnel.
