# Protocole d'Exécution & Tests de Validation (Phase 0 à Phase 3)

Ce document détaille les commandes de déploiement Ansible et l'ensemble des tests de validation de bout en bout (**Phase 0 à Phase 3**) pour la migration Kafka ZooKeeper vers KRaft avec sécurité mTLS et Kafbat UI.

---

## Utilisation en une ligne par phase

### Valider Phase 2, 3(actuelle)

./validate-migration.sh phase2
./validate-migration.sh phase3
./validate-migration.sh cleanup