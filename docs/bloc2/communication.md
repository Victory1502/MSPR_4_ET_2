# Processus de communication et animation à distance

## 1. Communication inclusive régulière au sein de l'équipe

Principes appliqués par l'équipe dans ses échanges :
- **Écoute active** : chaque point bloquant soulevé en Daily Meeting est reformulé par la personne qui reçoit l'information avant d'être traité, pour vérifier la bonne compréhension.
- **Reformulation fidèle** : les comptes-rendus écrits (Trello, canal de discussion) reprennent les termes utilisés par l'auteur sans réinterprétation, puis sont validés par l'auteur avant diffusion.
- **Volet international** : les livrables techniques clés (schéma d'architecture, README) sont doublés d'un résumé en anglais, pour s'entraîner à restituer fidèlement le discours technique en anglais dans un contexte multiculturel (cf. intitulé de la MSPR).

## 2. Outil collaboratif retenu
- **Discord** comme outil de visioconférence et de fil de discussion continu (channels : `#general`, `#data-engineering`, `#data-analyse`, `#data-science`, `#blocages`).
- **GitHub Discussions / Issues** pour tout ce qui doit rester traçable et lié au code.
- **Trello** pour le suivi visuel des tâches (cf. [outillage_agile.md](outillage_agile.md)).

Adaptabilité de l'outil : Discord permet sous-titres automatiques et retranscription texte pour les collaborateurs en situation de handicap auditif ; en cas de besoin, un outil de visio alternatif (Teams, qui propose une transcription native plus poussée) est proposé en remplacement.

## 3. Fil de discussion — process check-in / check-out
- **Check-in** (début de journée, canal `#general`) : chaque membre indique en 1 message sa disponibilité du jour, ses priorités, ses éventuels empêchements.
- **Check-out** (fin de journée) : chaque membre résume ce qui a été terminé, ce qui est reporté, et les blocages restants — alimente directement la rétrospective et le planning du lendemain.

## 4. Animation des réunions à distance

Support d'animation du Daily Meeting :
1. Ouverture (1 min) : rappel de l'objectif du jour (jalon du planning).
2. Tour de table structuré (10 min) : fait / en cours / bloqué, un membre à la fois.
3. Priorisation collective (3 min) : réajustement du board Trello en direct, partagé à l'écran.
4. Clôture (1 min) : rappel du prochain rendez-vous et des livrables attendus.

Outils numériques d'animation envisagés pour dynamiser les points d'équipe plus longs (revue de sprint) : **Klaxoon** (tableau collaboratif pour la rétrospective visuelle) ou **Padlet** (mur collaboratif asynchrone pour les retours qui ne peuvent pas être donnés en synchrone).

Aménagements liés aux 6 grandes familles de handicap (cf. [organisation_equipe.md](organisation_equipe.md) section 4) :
- Visuel : contenu partagé à l'écran toujours doublé d'une description orale.
- Auditif : activation systématique des sous-titres automatiques, compte-rendu écrit systématique.
- Moteur : contributions possibles en asynchrone sur Trello/Discord plutôt qu'en temps réel uniquement.
- Psychique / Mental / Invalidant : ordre du jour fixe et communiqué à l'avance, pas d'improvisation surprise, rythme annoncé.

## 5. Stratégie de partage d'information

| Type d'information | Outil | Fréquence |
|---|---|---|
| Avancement des tâches | Trello | Continu |
| Code, documentation technique | GitHub (repo `velibdata`) | Continu (à chaque commit/PR) |
| Échanges informels, blocages urgents | Discord | Continu |
| Compte-rendu de Daily Meeting | Discord `#general` (message épinglé) | Quotidien |
| Support de présentation finale | Fichier partagé (Google Slides / PowerPoint) versionné dans le repo | À J-1 de la soutenance |

Schéma d'utilisation : Trello (quoi faire) ↔ GitHub (comment c'est fait, code source de vérité) ↔ Discord (comment on en parle) — les trois outils sont interconnectés par convention de nommage (nom de tâche identique entre carte Trello et branche GitHub).
