# Manifests Kubernetes — VelibData

Ces manifestes decrivent la meme architecture que `docker-compose.yml` (cluster Kafka 3 brokers,
MinIO distribue 4 noeuds, PostgreSQL primary/replica, Spark master/workers), sous une forme
orchestree et **auto-scalable**, pour repondre au critere Bloc 4 « dimensionner en temps reel
les besoins en consommation de ressources en mettant en place l'autoscaling ».

Fournis comme infrastructure-as-code de reference : testables sur un cluster local (minikube,
k3d, kind) ou un cluster managé. Ils n'ont pas ete deployes en continu dans le cadre du MVP
(6 jours de preparation, cf. planning.md) — la stack quotidienne tourne sur `docker-compose.yml`.

## Prerequis
- Un cluster Kubernetes accessible (`kubectl config current-context`).
- Un `StorageClass` par defaut (pour les `volumeClaimTemplates`).
- **metrics-server** installe (requis par le `HorizontalPodAutoscaler` de `13-spark.yaml`) :
  `kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml`

## Deploiement
```bash
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-config.yaml

# Secrets : ne jamais committer de valeurs reelles (cf. 02-secret.example.yaml)
cp 02-secret.example.yaml 02-secret.yaml
# -> editer 02-secret.yaml avec des identifiants propres a l'environnement
kubectl apply -f 02-secret.yaml

# Scripts SQL/replication : generer le ConfigMap depuis les fichiers source de verite (mspr-tech/sql/)
kubectl create configmap postgres-init-scripts -n velibdata \
  --from-file=01_init_schema.sql=../sql/init_schema.sql \
  --from-file=02_init_replication.sh=../sql/init_replication.sh \
  --from-file=replica-entrypoint.sh=../sql/replica-entrypoint.sh \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f 10-kafka.yaml
kubectl apply -f 11-minio.yaml
kubectl apply -f 12-postgres.yaml
kubectl apply -f 13-spark.yaml

kubectl get pods -n velibdata -w
```

## Verifier l'autoscaling
```bash
kubectl get hpa spark-worker-hpa -n velibdata -w
# Generer de la charge CPU sur les workers (ex: soumettre plusieurs jobs Spark en parallele)
# puis observer REPLICAS passer de 2 a 6 (et redescendre apres stabilisation, cf. behavior.scaleDown).
```

## Validation de ces manifestes
- Syntaxe YAML verifiee localement avec PyYAML (`yaml.safe_load_all`).
- Schema Kubernetes verifie hors-ligne (sans cluster reel) dans la chaine CI via
  [kubeconform](https://github.com/yannh/kubeconform) — cf. `.github/workflows/ci.yml`, job `validate-k8s`.

## Correspondance avec docker-compose.yml
| docker-compose.yml | Equivalent Kubernetes |
|---|---|
| kafka-1/2/3 (3 services) | StatefulSet `kafka` (3 replicas) + Service headless |
| minio1..4 + minio-lb (nginx) | StatefulSet `minio` (4 replicas) + Service `minio` (LB natif) |
| postgres-primary / postgres-replica | StatefulSet `postgres-primary` / `postgres-replica` |
| spark-master / spark-worker-1..3 (fixe) | Deployment `spark-master` / Deployment `spark-worker` + HPA (2 a 6, dynamique) |
| Prometheus/Grafana/exporters | Non repris ici (MVP) — pistes : kube-prometheus-stack (Helm) |
