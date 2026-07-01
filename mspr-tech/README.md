# VélibData — mspr-tech

Infrastructure et pipelines de données du projet VélibData. Vue d'ensemble de l'architecture : [../docs/bloc4/architecture.md](../docs/bloc4/architecture.md).

Architecture en **cluster tolérant aux pannes** : Kafka (3 brokers KRaft), MinIO (4 nœuds, erasure coding), PostgreSQL (primary + réplique), Spark (1 master + 3 workers), supervisés par Prometheus/Grafana. Détails et procédures de bascule : [../docs/bloc4/architecture.md section 5](../docs/bloc4/architecture.md#5-cluster-tolérant-aux-pannes-zéro-panne).

## 1. Prérequis
- Docker Desktop (Compose v2)
- Python 3.11+ (un environnement virtuel est déjà présent dans ce dossier)

## 2. Démarrage rapide

```bash
# 1. Configurer les secrets (jamais commités)
cp .env.example .env
# -> éditer .env avec des identifiants propres à votre poste

# 2. Démarrer l'infrastructure complète (cluster Kafka/MinIO/Postgres/Spark + monitoring)
docker compose up -d
docker compose ps   # tous les services doivent passer "healthy" (~1-2 min, cluster complet)

# 3. Installer les dépendances Python (producteur + utilitaires)
python -m venv .   # si l'environnement n'existe pas déjà
./Scripts/pip install -r requirements.txt   # Windows : Scripts/python.exe -m pip install -r requirements.txt

# 4. Lancer le producteur (ingestion continue des 2 API Vélib')
python producers/kafka_producer.py

# 5. Dans un autre terminal : lancer le traitement Spark (micro-batch à la demande)
bash spark_jobs/run_job.sh
```

## 3. Vérifier que les données sont bien arrivées

```bash
# Données normalisées (couche silver, sur le primary)
docker exec mspr-tech-postgres-primary-1 psql -U velib_app -d velibdata -c "SELECT count(*) FROM stations;"
docker exec mspr-tech-postgres-primary-1 psql -U velib_app -d velibdata -c "SELECT count(*) FROM disponibilite_releve;"
docker exec mspr-tech-postgres-primary-1 psql -U velib_app -d velibdata -c "SELECT * FROM pipeline_alertes ORDER BY detected_at DESC LIMIT 5;"

# Vérifier la réplication PostgreSQL (doit lister postgres-replica avec un lag proche de 0)
docker exec mspr-tech-postgres-primary-1 psql -U velib_app -d velibdata -c "SELECT * FROM pg_stat_replication;"

# Archive brute (couche bronze) - via le load-balancer MinIO
docker exec mspr-tech-minio1-1 mc alias set local http://minio-lb:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>
docker exec mspr-tech-minio1-1 mc ls --recursive local/velib-data
```

Interfaces web :
| Interface | URL |
|---|---|
| MinIO Console (load-balancée sur les 4 nœuds) | http://localhost:9001 |
| Spark Master UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana (dashboard "VelibData - Vue d'ensemble infrastructure") | http://localhost:3000 |

## 4. Tester la tolérance aux pannes

```bash
# Couper un broker Kafka (1 sur 3) : le pipeline continue sans interruption
docker compose stop kafka-2
docker compose ps kafka-1 kafka-3   # toujours "healthy"
docker compose start kafka-2        # le broker resynchronise ses partitions au retour

# Couper un nœud MinIO (1 sur 4) : l'erasure coding tolère la perte
docker compose stop minio3
docker exec mspr-tech-minio1-1 mc ls --recursive local/velib-data   # toujours accessible
docker compose start minio3

# Couper le primary PostgreSQL : la réplique reste consultable (lecture seule)
docker compose stop postgres-primary
docker exec mspr-tech-postgres-replica-1 psql -U velib_app -d velibdata -c "SELECT count(*) FROM stations;"
docker compose start postgres-primary
```

## 5. Structure du dossier

```
mspr-tech/
├── docker-compose.yml       # Cluster Kafka(x3)/MinIO(x4)/Postgres(primary+replica)/Spark(x4) + monitoring
├── requirements.txt         # Dépendances Python (producteur + job Spark côté driver)
├── requirements-dev.txt     # Dépendances de developpement/CI (pytest, flake8)
├── pytest.ini               # Configuration pytest (tests unitaires des règles de qualité)
├── .env.example             # Modèle de configuration (copier en .env, jamais commité)
├── producers/
│   └── kafka_producer.py    # Ingestion : API Vélib' -> topics Kafka
├── spark_jobs/
│   ├── quality_rules.py           # Règles de qualité (fonctions pures, testées en CI)
│   ├── velib_batch_processing.py  # Transformation, qualité, alertes, écriture MinIO+Postgres
│   └── run_job.sh                 # Lance le job dans le conteneur spark-master
├── sql/
│   ├── init_schema.sql          # Modèle de données normalisé (chargé au démarrage de Postgres)
│   ├── init_replication.sh      # Crée le rôle de réplication (primary)
│   └── replica-entrypoint.sh    # Clone le primary via pg_basebackup (réplique)
├── minio/
│   └── nginx.conf            # Load-balancer devant les 4 nœuds MinIO distribués
├── monitoring/
│   ├── prometheus.yml               # Scrape config (Kafka, Postgres, conteneurs, Spark)
│   ├── spark-metrics.properties     # Active l'export Prometheus des daemons Spark
│   └── grafana/                     # Provisioning datasource + dashboard
├── k8s/                      # Manifestes Kubernetes équivalents (StatefulSets + HPA autoscaling)
│   └── README.md             # Déploiement, prérequis (metrics-server), correspondance avec compose
└── tests/
    └── test_quality_rules.py # Tests unitaires (13 cas), exécutés en CI (.github/workflows/ci.yml)
```

## 6. Intégration continue

Chaque push/pull request déclenche `.github/workflows/ci.yml` : lint (flake8) + tests unitaires (pytest) des règles de qualité, validation de `docker-compose.yml`, et validation des manifestes Kubernetes (schéma officiel, hors-ligne). Détails : [../docs/bloc4/architecture.md section 7](../docs/bloc4/architecture.md#7-chaîne-dintégration-continue-cicd).

Pour lancer les tests en local :
```bash
./Scripts/pip install -r requirements-dev.txt   # Windows : Scripts/python.exe -m pip install -r requirements-dev.txt
./Scripts/python.exe -m pytest -v
./Scripts/python.exe -m flake8 --max-line-length=120 --extend-ignore=E203,W503 producers spark_jobs tests
```

## 7. En cas de problème
Voir le runbook de maintenance curative : [../docs/bloc4/plan_maintenance.md](../docs/bloc4/plan_maintenance.md).
