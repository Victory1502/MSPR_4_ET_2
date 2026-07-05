# Analytics — Prototypes Data Analyst / Data Scientist

Livrables métier T7 (dashboard), T8 (statistiques descriptives) et T9 (prédiction), cf.
[`docs/bloc2/planning.md`](../../docs/bloc2/planning.md).

## Contenu

```
analytics/
├── requirements.txt          # Dependances specifiques (pandas, scikit-learn, jupyter...)
└── notebooks/
    ├── 01_statistiques_descriptives.ipynb   # T8 - Lucas (Data Analyst)
    └── 02_prediction_demande.ipynb          # T9 - Lyes (Data Scientist)
```

Le livrable T7 (dashboard d'usage) n'est pas ici : c'est un tableau de bord **Grafana**,
provisionné automatiquement au démarrage de `docker compose` — cf.
[`monitoring/grafana/dashboards/velibdata-usage.json`](../monitoring/grafana/dashboards/velibdata-usage.json),
accessible sur `http://localhost:3000` (datasource dédiée `PostgreSQL - VelibData`, distincte
des exporters infra du bloc 4).

## Exécution en local

```bash
cd mspr-tech
./Scripts/pip install -r analytics/requirements.txt   # Windows : Scripts/python.exe -m pip install -r analytics/requirements.txt

# Kernel Jupyter dedie a ce venv (une seule fois)
./Scripts/python.exe -m ipykernel install --user --name mspr-tech-venv --display-name "MSPR VelibData (mspr-tech venv)"

# Ouvrir les notebooks
./Scripts/python.exe -m jupyter notebook analytics/notebooks/
```

Les notebooks lisent la configuration depuis `../../.env` (mêmes identifiants que le reste du
projet) et se connectent à `postgres-primary` (couche silver : `stations`, `disponibilite_releve`,
`pipeline_alertes`).

**Driver PostgreSQL : `pg8000` (pur Python), pas `psycopg2`** — `psycopg2`/libpq plante avec un
`UnicodeDecodeError` sur Windows lorsque le chemin du projet contient des caractères accentués
(bug constaté localement). `pg8000` n'a pas cette dépendance C et évite le problème.

## Limite assumée

Les résultats (statistiques et modèle de prédiction) ont été produits avec l'historique
disponible au moment de la rédaction (~2 jours, 3107 relevés). C'est suffisant pour valider la
mécanique du pipeline (requêtes, feature engineering, entraînement, évaluation) mais **pas**
pour un modèle de production — celui-ci devra être ré-entraîné une fois plusieurs semaines
d'historique accumulées (cf. conclusion du notebook `02_prediction_demande.ipynb`).
