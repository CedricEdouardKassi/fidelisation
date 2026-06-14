# Modèle de Données

## Schéma Unity Catalog

Toutes les tables sont stockées dans `${catalog}.${schema}` (par défaut : `training.fidelisation`).

## Tables Source

### `commerciaux`

L'équipe commerciale avec leurs affectations régionales.

| Colonne | Type | Description |
|---------|------|-------------|
| `commercial_id` | STRING | Identifiant unique (COM-001 à COM-020) |
| `prenom` | STRING | Prénom |
| `nom` | STRING | Nom de famille |
| `email` | STRING | Email professionnel |
| `region` | STRING | Région d'affectation (IDF, Lyon, Marseille, ...) |
| `nb_clients_portefeuille` | INT | Nombre de clients en portefeuille |
| `objectif_renouvellement_mensuel` | INT | Objectif mensuel de renouvellements |
| `date_embauche` | STRING | Date d'embauche (YYYY-MM-DD) |

### `clients`

Le portefeuille B2B avec informations contractuelles.

| Colonne | Type | Description |
|---------|------|-------------|
| `client_id` | STRING | Identifiant unique (CLI-0001 à CLI-0500) |
| `raison_sociale` | STRING | Nom de l'entreprise |
| `segment` | STRING | TPE, PME, ETI, Grand Compte, Collectivité |
| `ville` | STRING | Ville du siège |
| `commercial_id` | STRING | FK → commerciaux |
| `offre_actuelle` | STRING | Offre souscrite |
| `valeur_contrat_annuel` | DOUBLE | CA annuel du contrat (€) |
| `date_contrat` | STRING | Date de début (YYYY-MM-DD) |
| `date_fin_contrat` | STRING | Date de fin (YYYY-MM-DD) |
| `duree_contrat_mois` | INT | Durée contractuelle (12/24/36) |
| `renouvellement_auto` | BOOLEAN | Renouvellement automatique activé |
| `score_nps` | DOUBLE | Net Promoter Score (1-10) |
| `anciennete_mois` | INT | Ancienneté client en mois |
| `nb_reclamations_12m` | INT | Réclamations sur 12 mois |
| `potentiel_upsell` | DOUBLE | Potentiel de montée en gamme (€) |
| `canal_prefere` | STRING | Canal de communication préféré |
| `score_initial_manuel` | INT | Score initial attribué manuellement (1-5) |

### `historique_appels`

Tous les appels des 12 derniers mois.

| Colonne | Type | Description |
|---------|------|-------------|
| `appel_id` | STRING | Identifiant unique |
| `client_id` | STRING | FK → clients |
| `commercial_id` | STRING | FK → commerciaux |
| `date_appel` | STRING | Date (YYYY-MM-DD) |
| `heure_appel` | STRING | Heure (HH:MM) |
| `heure_slot` | INT | Créneau horaire (9-18) |
| `jour_semaine` | STRING | Jour (Lun-Ven) |
| `direction` | STRING | sortant / entrant |
| `resultat` | STRING | décroché, absent, messagerie, occupé |
| `duree_secondes` | INT | Durée de l'appel (0 si non décroché) |
| `canal` | STRING | telephone, email, sms |
| `notes_commerciales` | STRING | Notes libres du commercial |

### `agenda_commerciaux`

Disponibilités des commerciaux sur J+30.

| Colonne | Type | Description |
|---------|------|-------------|
| `commercial_id` | STRING | FK → commerciaux |
| `date_slot` | STRING | Date du créneau (YYYY-MM-DD) |
| `jour_semaine` | STRING | Jour (Lun-Ven) |
| `heure_slot` | INT | Créneau horaire (9-17) |
| `disponible` | BOOLEAN | Disponible pour appels sortants |
| `motif_indisponibilite` | STRING | Motif si indisponible |

### `signaux_contrats`

Signaux déclencheurs actifs pour la fidélisation.

| Colonne | Type | Description |
|---------|------|-------------|
| `client_id` | STRING | FK → clients |
| `type_signal` | STRING | echeance_contrat, nps_bas, silence_client |
| `valeur_signal` | STRING | Détail du signal (J-15, NPS=3.2, ...) |
| `score_signal_pts` | INT | Points attribués au signal (1-8) |
| `date_signal` | STRING | Date de détection |
| `jours_avant_echeance` | INT | Jours avant fin de contrat |
| `renouvellement_auto` | BOOLEAN | Renouvellement auto actif |
| `offre_trimestrielle_active` | BOOLEAN | Offre promo en cours |
| `campagne_en_cours` | BOOLEAN | Campagne marketing active |
| `saisonnalite_detectee` | BOOLEAN | Pattern saisonnier détecté |
| `score_ml_propension` | DOUBLE | Score ML de propension (0-1) |

### `heatmap_joignabilite`

Matrice de taux de réponse par créneau (agrégé tous clients).

| Colonne | Type | Description |
|---------|------|-------------|
| `jour_semaine` | STRING | Jour (Lun-Ven) |
| `heure_slot` | INT | Créneau horaire (9-18) |
| `taux_reponse_moyen` | DOUBLE | Taux de réponse moyen (0-1) |
| `nb_appels_historique` | INT | Nombre d'appels dans ce créneau |
| `nb_reponses` | INT | Nombre de réponses |

## Tables Pipeline (générées par SDP)

### `silver_clients_features`

Features enrichies par client (jointure clients + appels + signaux).

Colonnes ajoutées par rapport à `clients` :
- `nb_appels_12m`, `nb_decroches_12m`, `nb_appels_sortants_12m`
- `taux_reponse_historique`, `jours_depuis_dernier_contact`
- `jours_avant_echeance`, `score_signal_pts`, `score_ml_propension`
- `malus_auto_off`, `offre_trimestrielle_active`, `campagne_en_cours`, `saisonnalite_detectee`
- **`score_urgence`**, **`score_joignabilite`**, **`score_valeur`**, **`score_signal_marketing`**

### `gold_priorites_clients`

Liste finale scorée et priorisée.

Colonnes ajoutées par rapport à `silver_clients_features` :
- `score_priorite` — Score composite final
- `rang_priorite_region` — Rang au sein de la région
- `a_appeler_aujourdhui` — Flag top 20 par région
- `meilleur_jour_appel` — Prochain créneau disponible
- `heure_recommandee` — Heure optimale (ex: "16h00")
- `commercial_nom` — Nom complet du commercial

### `gold_dashboard_jour`

KPIs agrégés pour le dashboard (1 ligne).

| Colonne | Type | Description |
|---------|------|-------------|
| `nb_clients_a_appeler` | INT | Total clients flaggés aujourd'hui |
| `nb_contrats_j30` | INT | Contrats arrivant à échéance J-30 |
| `taux_joignabilite_moyen` | DOUBLE | Taux moyen de joignabilité (%) |
| `ca_potentiel_en_jeu` | DOUBLE | CA total des clients à appeler (€) |

## Relations

```
commerciaux (1) ──── (N) clients
commerciaux (1) ──── (N) agenda_commerciaux
clients     (1) ──── (N) historique_appels
clients     (1) ──── (N) signaux_contrats
commerciaux (1) ──── (N) historique_appels
```
