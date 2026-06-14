# Algorithme de Scoring

## Objectif

Calculer un **score composite de priorité** pour chaque client afin de déterminer l'ordre optimal d'appel quotidien par commercial. Le score maximise la probabilité de renouvellement en combinant urgence, joignabilité, valeur et signaux marketing.

## Formule du score composite

```
score_priorité = (score_urgence × 0.35)
               + (score_joignabilité × 0.30)
               + (score_valeur × 0.20)
               + (score_signal_marketing × 0.15)
```

## Composantes détaillées

### 1. Score d'urgence (poids : 35%)

Basé sur la proximité de l'échéance contractuelle :

| Condition | Score de base | Avec malus auto_off |
|-----------|:---:|:---:|
| J-15 (≤ 15 jours) | 8 | 16 |
| J-30 (≤ 30 jours) | 4 | 8 |
| J-60 (≤ 60 jours) | 2 | 4 |
| J-90 (≤ 90 jours) | 1 | 2 |
| > 90 jours | 0 | 0 |

**Malus renouvellement auto OFF** : Si le client n'a PAS le renouvellement automatique, le score d'urgence est doublé (×2). Ces clients nécessitent une action commerciale explicite.

### 2. Score de joignabilité (poids : 30%)

Évalue la probabilité d'atteindre le client :

```
score_joignabilité = (taux_réponse_historique × 6)
                   + (bonus_inactivité)
                   + (bonus_canal_téléphone)
```

| Composante | Calcul | Plage |
|-----------|--------|-------|
| Taux réponse historique | nb_décrochés / nb_appels × 6 | 0 – 6 |
| Bonus inactivité | +2 si dernier contact > 30 jours | 0 ou 2 |
| Bonus canal préféré | +2 si téléphone, +1 sinon | 1 – 2 |

### 3. Score de valeur (poids : 20%)

Quantifie l'enjeu financier et la qualité de la relation :

```
score_valeur = (CA_annuel / 100 000 × 5)
             + (potentiel_upsell / 50 000 × 3)
             + (bonus_NPS_élevé)
             - (nb_réclamations × 0.5)
```

| Composante | Calcul | Impact |
|-----------|--------|--------|
| Valeur contrat | CA / 100k × 5 | Proportionnel |
| Potentiel upsell | Potentiel / 50k × 3 | Proportionnel |
| NPS élevé | +1 si NPS ≥ 8 | Bonus fidélité |
| Réclamations | -0.5 par réclamation | Malus risque |

### 4. Score signal marketing (poids : 15%)

Capture les opportunités contextuelles :

```
score_signal_marketing = score_signal_pts
                       + offre_trimestrielle_active × 1
                       + campagne_en_cours × 1
                       + saisonnalité_détectée × 1
                       + score_ML_propension × 2
```

| Signal | Points |
|--------|:---:|
| Score signal base (échéance) | 1 – 8 |
| Offre trimestrielle active | +1 |
| Campagne en cours | +1 |
| Saisonnalité détectée | +1 |
| Score ML propension (0-1) | ×2 |

## Priorisation finale

1. **Score composite** calculé pour tous les clients avec un signal actif
2. **Classement par région** via `rang_priorite_region` (RANK over PARTITION BY region)
3. **Sélection quotidienne** : top 20 clients par région → `a_appeler_aujourdhui = TRUE`
4. **Créneau recommandé** : meilleur slot disponible du commercial (agenda × heatmap joignabilité)

## Exemple concret

| Client | Urgence | Joignabilité | Valeur | Marketing | **Score** |
|--------|:---:|:---:|:---:|:---:|:---:|
| CLI-0042 (ETI, J-12, auto OFF) | 16×0.35=5.60 | 7.2×0.30=2.16 | 4.8×0.20=0.96 | 6.5×0.15=0.98 | **9.70** |
| CLI-0187 (PME, J-28) | 4×0.35=1.40 | 5.0×0.30=1.50 | 2.1×0.20=0.42 | 3.0×0.15=0.45 | **3.77** |

## Axes d'amélioration

- Intégration du score ML de propension au churn comme composante à part entière
- Pondération dynamique par segment (Grand Compte vs TPE)
- Feedback loop : ajuster les poids selon le taux de renouvellement observé
- A/B testing des seuils d'urgence
