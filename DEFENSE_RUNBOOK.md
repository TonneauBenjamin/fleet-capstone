# Soutenance du 21 Octobre — Guide & Commandes Directes (V1)
**Date** : 21 Octobre 2026 (10 minutes)  
**Binôme** : Nathan CHEVAUCHE & Benjamin TONNEAU  
**Dossier** : `/Users/benjamintonneau/Downloads/capstone_v1`

---

## 1. État des lieux : Tout tourne sur votre Mac !

Le dossier `capstone_v1` est **la version exacte et ciblée pour le jalon du 21 octobre**.
Il contient :
* Kafka 3.8 en mode KRaft
* Le conteneur d'initialisation (création des 6 topics + publication du catalogue)
* Le simulateur à 20 messages/seconde
* Les 2 notificateurs (`notifier1` et `notifier2`) avec bascule sans perte (`cooperative-sticky`)
* Le job Spark Streaming avec ses 4 requêtes (validation DLQ, Parquet + alertes, moyennes 1 min, excès prolongé 5 min)
* Le document de suivi officiel : `V1_progress_review.pdf`

---

## 2. Commandes à taper en direct pendant la soutenance

Ouvrez un terminal dans `/Users/benjamintonneau/Downloads/capstone_v1`.

### A. Démarrer la plateforme (si pas déjà fait)
```bash
docker compose up -d
docker compose ps
```
*(Tous les conteneurs doivent être `Up` / `Healthy`).*

---

### B. Démo Kafka & Notificateurs (Nathan)

1. **Voir les partitions assignées aux deux notificateurs :**
```bash
docker compose logs notifier1
docker compose logs notifier2
```
*(Vous verrez par exemple `notifier1` sur la partition [1] et `notifier2` sur [0, 2]).*

2. **Démontrer la tolérance aux pannes (tuer un notificateur en direct) :**
```bash
docker compose kill notifier2
docker compose logs -f notifier1
```
*(Après ~45 secondes, `notifier1` récupère les partitions sans perdre le moindre message).*

3. **Le redémarrer :**
```bash
docker compose start notifier2
```
*(La partition revient automatiquement vers `notifier2` sans interruption).*

4. **Vérifier le retard (Lag = 0) :**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerts-notifier
```

---

### C. Démo Spark Streaming (Nathan & Benjamin)

Ouvrez l'interface Spark dans Chrome : **[http://localhost:4040](http://localhost:4040)**

1. **Montrer la boîte aux lettres mortes (DLQ) :**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle_events_dlq --from-beginning --max-messages 3 --property print.key=true
```
*(Montrez au prof les causes d'erreurs détectées : `invalid_json`, `speed_out_of_range`, `missing_vehicle_id`).*

2. **Montrer les indicateurs régionaux calculés toutes les minutes (Query 2) :**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic region_kpis_1min --from-beginning --max-messages 5 --property print.key=true
```

3. **Déclencher la démo d'excès de vitesse prolongé (Query 3) :**
```bash
docker compose run --rm --no-deps simulator python producers/scenario_sustained_speeding.py --vehicle V0007 --seconds 30 --rate 4
```
Puis afficher l'alerte générée :
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning --timeout-ms 5000 | grep "sustained_speeding" | head -n 3
```

4. **Démo véhicule inconnu (Tombstone) :**
```bash
echo "V0040:NULL" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic fleet_catalog --property parse.key=true --property key.separator=: --property null.marker=NULL
```
*(Le véhicule V0040 supprimé du catalogue reçoit la région `UNKNOWN` au micro-batch suivant au lieu de disparaître).*

---

## 3. Ce que vous annoncez pour le 7 novembre (Next Steps)

Terminez votre présentation en montrant la section **4. Status / Next steps** de votre `V1_progress_review.pdf` :
* La mise en place des jobs Spark Batch (test qualité et rapport journalier avec élagage de partitions).
* L'étude du déséquilibre de charge (*salting* vs *AQE*).
* L'orchestration automatique avec Apache Airflow (DAG quotidien avec capteur en mode *reschedule* et surveillance du lag).
* Les expériences de résilience aux pannes et le rapport final.
