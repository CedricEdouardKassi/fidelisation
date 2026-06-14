"""
Logique de scoring de fidélisation — fonctions pures Python.

Implémente les formules documentées dans docs/scoring.md.
Ces fonctions sont testables localement sans Spark ni Databricks,
et servent de référence pour l'implémentation SQL dans gold.sql.
"""

from __future__ import annotations


# =============================================================================
# SCORE D'URGENCE (poids 35%)
# =============================================================================

def score_urgence(jours_avant_echeance: int, renouvellement_auto: bool) -> float:
    """
    Score basé sur la proximité de l'échéance contractuelle.
    Doublé si le renouvellement automatique est désactivé (action commerciale obligatoire).
    """
    if jours_avant_echeance < 0 or jours_avant_echeance > 90:
        base = 0
    elif jours_avant_echeance <= 15:
        base = 8
    elif jours_avant_echeance <= 30:
        base = 4
    elif jours_avant_echeance <= 60:
        base = 2
    else:
        base = 1

    return float(base * (2 if not renouvellement_auto and base > 0 else 1))


# =============================================================================
# SCORE DE JOIGNABILITÉ (poids 30%)
# =============================================================================

def score_joignabilite(
    taux_reponse_historique: float,
    jours_depuis_dernier_contact: int,
    canal_prefere: str,
) -> float:
    """
    Probabilité d'atteindre le client basée sur l'historique de contact.

    taux_reponse_historique : nb_décroches / nb_appels, entre 0 et 1
    jours_depuis_dernier_contact : +2 bonus si > 30 jours (client à recontacter)
    canal_prefere : +2 si téléphone, +1 sinon
    """
    score = taux_reponse_historique * 6.0
    if jours_depuis_dernier_contact > 30:
        score += 2.0
    score += 2.0 if canal_prefere == "telephone" else 1.0
    return score


# =============================================================================
# SCORE DE VALEUR (poids 20%)
# =============================================================================

def score_valeur(
    valeur_contrat_annuel: float,
    potentiel_upsell: float,
    score_nps: float,
    nb_reclamations: int,
) -> float:
    """
    Enjeu financier et qualité de la relation client.
    Plancher à 0 pour éviter un score négatif.
    """
    score = (valeur_contrat_annuel / 100_000.0) * 5.0
    score += (potentiel_upsell / 50_000.0) * 3.0
    if score_nps >= 8.0:
        score += 1.0
    score -= nb_reclamations * 0.5
    return max(0.0, score)


# =============================================================================
# SCORE SIGNAL MARKETING (poids 15%)
# =============================================================================

def score_signal_marketing(
    score_signal_pts: float,
    offre_trimestrielle_active: bool,
    campagne_en_cours: bool,
    saisonnalite_detectee: bool,
    score_ml_propension: float,
) -> float:
    """
    Opportunités contextuelles : signaux contrats + levier ML.
    score_ml_propension : valeur entre 0 et 1, pondérée ×2.
    """
    score = float(score_signal_pts)
    score += 1.0 if offre_trimestrielle_active else 0.0
    score += 1.0 if campagne_en_cours else 0.0
    score += 1.0 if saisonnalite_detectee else 0.0
    score += score_ml_propension * 2.0
    return score


# =============================================================================
# SCORE COMPOSITE FINAL (priorité)
# =============================================================================

def score_priorite(
    sc_urgence: float,
    sc_joignabilite: float,
    sc_valeur: float,
    sc_signal_marketing: float,
) -> float:
    """
    Score composite pondéré :
      0.35 × urgence + 0.30 × joignabilité + 0.20 × valeur + 0.15 × marketing
    """
    return (
        sc_urgence * 0.35
        + sc_joignabilite * 0.30
        + sc_valeur * 0.20
        + sc_signal_marketing * 0.15
    )


# =============================================================================
# CLASSIFICATION DES SIGNAUX D'URGENCE
# =============================================================================

def classify_urgence_signal(jours_avant_fin: int) -> tuple[str, int] | None:
    """
    Retourne (label, score_pts) si le contrat arrive à échéance dans 90 jours,
    None sinon.

    Seuils : J-15 → 8 pts, J-30 → 4 pts, J-60 → 2 pts, J-90 → 1 pt.
    """
    if jours_avant_fin < 0 or jours_avant_fin > 90:
        return None
    if jours_avant_fin <= 15:
        return ("J-15", 8)
    if jours_avant_fin <= 30:
        return ("J-30", 4)
    if jours_avant_fin <= 60:
        return ("J-60", 2)
    return ("J-90", 1)
