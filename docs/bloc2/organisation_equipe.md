# Organisation de l'équipe projet — VélibData

## 1. Structure de l'équipe et rôles

| Membre | Rôle | Responsabilités principales |
|---|---|---|
| **Victory** | Data Engineer | Architecture technique, pipelines de données (Kafka/Spark/MinIO), mise en place des alertes qualité/panne, mise à disposition des outils aux autres équipes |
| **Belkis** | Data Analyst | Tableaux de bord de suivi de l'utilisation, analyse des tendances Vélib' |
| **Lucas** | Data Analyst | Statistiques descriptives sur la performance des stations, contrôle qualité fonctionnelle des données livrées |
| **Lyes** | Data Scientist | Modèles de prédiction de la demande, système de recommandation |

Le rôle de **Scrum Master / coordination agile** est tenu de façon tournante par Victory sur cette itération (animation des daily meetings, mise à jour du board Trello), avec possibilité de rotation à chaque sprint suivant les principes d'amélioration continue.

## 2. Matrice RACI

| Activité | Victory (DE) | Belkis (DA) | Lucas (DA) | Lyes (DS) |
|---|---|---|---|---|
| Architecture & déploiement infra | R/A | C | C | I |
| Pipeline d'ingestion (Kafka) | R/A | I | I | I |
| Qualité des données & alertes | R/A | C | C | C |
| Modélisation des données | R/A | C | C | C |
| Tableaux de bord d'usage | C | R/A | R | I |
| Modèles de prédiction / recommandation | I | C | C | R/A |
| Documentation technique | R/A | C | C | C |
| Support de présentation finale | C | R | R | R |

*R = Réalisateur, A = Approbateur, C = Consulté, I = Informé*

## 3. Gestion des changements de priorité et des imprévus

- Le backlog Trello est revu chaque matin (Daily Meeting, 15 min) et peut être réordonné en fonction des blocages remontés (ex : API externe indisponible, panne d'un conteneur).
- **Scénarios possibles envisagés** :
  1. *Panne d'un service Docker (Kafka/MinIO/Spark)* → bascule sur les tests avec les données déjà historisées, ticket de blocage créé sur Trello, tâche réassignée à Victory en priorité.
  2. *API opendata.paris.fr indisponible ou en erreur* → utilisation du dernier snapshot stocké dans MinIO pour ne pas bloquer les Data Analysts/Scientists.
  3. *Retard sur une tâche technique bloquante* → réaffectation temporaire d'un Data Analyst en binôme avec le Data Engineer (le sujet est jugé prioritaire sur la répartition initiale des rôles).
- **Personne relais en cas d'urgence** (absence, incident, question critique pendant la préparation) : Victory est désigné point de contact principal ; Belkis est la suppléante si Victory est indisponible.

## 4. Stratégie d'accueil et d'inclusion des situations de handicap

### 4.1 Les 6 grandes familles de handicap (référentiel AGEFIPH)
| Famille | Impacts possibles en contexte projet Data |
|---|---|
| Visuel | Difficulté à lire des dashboards denses, des schémas d'architecture non contrastés |
| Auditif | Difficulté à suivre les daily meetings vocaux, les réunions à distance sans sous-titrage |
| Moteur | Fatigue à l'utilisation prolongée du clavier/souris, besoin d'outils adaptés |
| Psychique | Besoin de rythmes de travail réguliers, sensibilité à la charge de stress (deadlines serrées) |
| Mental | Besoin de consignes simplifiées, séquencées, reformulées |
| Invalidant (maladies chroniques/invalidantes) | Fatigabilité variable, besoin de flexibilité horaire |

### 4.2 Cas concret représentatif retenu par l'équipe
Scénario préparé pour la MSPR : intégration d'un·e collaborateur/collaboratrice **en situation de handicap moteur** rejoignant l'équipe Data Analyst en cours de sprint.

Mesures mises en place :
- Aménagement du poste : clavier/souris adaptés, raccourcis clavier plutôt que manipulations répétées de la souris sur les tableaux de bord.
- Répartition des tâches : priorité aux tâches d'analyse et de conception de KPI (moins de manipulation intensive), tâches de saisie répétitive redistribuées.
- Aménagement du temps : horaires de daily meeting fixes mais durée de contribution asynchrone possible (mise à jour du Trello en différé accepté).
- Référent handicap de l'entreprise identifié comme point de contact pour tout aménagement complémentaire non anticipé par l'équipe.

### 4.3 Règles et bonnes pratiques d'inclusion adoptées
- Toute documentation (schémas, dashboards) est produite avec un contraste suffisant et des libellés textuels (pas uniquement des couleurs) pour rester lisible par les collaborateurs en situation de handicap visuel.
- Les réunions à distance sont enregistrées et un compte-rendu écrit est systématiquement partagé sur le fil de discussion (cf. [communication.md](communication.md)) pour les collaborateurs en situation de handicap auditif ou ne pouvant se connecter en synchrone.
- Les consignes de sprint sont toujours doublées d'un support écrit sur Trello (pas uniquement à l'oral) pour les besoins de reformulation.
- Les délais sont annoncés avec une marge tampon (cf. [planning.md](planning.md)) pour absorber les besoins de flexibilité horaire.
