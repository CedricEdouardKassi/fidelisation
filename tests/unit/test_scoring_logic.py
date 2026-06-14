"""
Tests unitaires — Logique de scoring composite.

Couvre les 5 fonctions de src/scoring/scoring.py sans dépendance Spark/Databricks.
Chaque règle métier de docs/scoring.md est testée explicitement.
"""

import pytest
from src.scoring.scoring import (
    score_urgence,
    score_joignabilite,
    score_valeur,
    score_signal_marketing,
    score_priorite,
)


# =============================================================================
# SCORE D'URGENCE
# =============================================================================

class TestScoreUrgence:

    # --- Valeurs nominales par seuil ---

    def test_j15_avec_renouvellement_auto(self):
        assert score_urgence(10, True) == 8.0

    def test_j15_sans_renouvellement_auto_double(self):
        assert score_urgence(10, False) == 16.0

    def test_j30(self):
        assert score_urgence(25, True) == 4.0

    def test_j30_sans_auto(self):
        assert score_urgence(25, False) == 8.0

    def test_j60(self):
        assert score_urgence(45, True) == 2.0

    def test_j60_sans_auto(self):
        assert score_urgence(45, False) == 4.0

    def test_j90(self):
        assert score_urgence(75, True) == 1.0

    def test_j90_sans_auto(self):
        assert score_urgence(75, False) == 2.0

    def test_au_dela_90_jours(self):
        assert score_urgence(100, True) == 0.0

    def test_au_dela_90_sans_auto_pas_de_doublement(self):
        # base = 0 → le doublement ne s'applique pas
        assert score_urgence(100, False) == 0.0

    def test_contrat_expire_jours_negatifs(self):
        assert score_urgence(-1, True) == 0.0

    # --- Frontières exactes ---

    @pytest.mark.parametrize("jours,expected", [
        (0,  8.0),
        (15, 8.0),
        (16, 4.0),  # bascule J-30
        (30, 4.0),
        (31, 2.0),  # bascule J-60
        (60, 2.0),
        (61, 1.0),  # bascule J-90
        (90, 1.0),
        (91, 0.0),  # hors fenêtre
    ])
    def test_frontieres(self, jours, expected):
        assert score_urgence(jours, True) == expected

    def test_retourne_float(self):
        assert isinstance(score_urgence(10, True), float)


# =============================================================================
# SCORE DE JOIGNABILITÉ
# =============================================================================

class TestScoreJoignabilite:

    def test_taux_zero_telephone_recente(self):
        # 0×6 + 0 (contact récent) + 2 (téléphone)
        assert score_joignabilite(0.0, 10, "telephone") == 2.0

    def test_taux_parfait_telephone(self):
        # 1.0×6 + 0 + 2 = 8.0
        assert score_joignabilite(1.0, 10, "telephone") == 8.0

    def test_bonus_inactivite_declenche_apres_30j(self):
        score_31j = score_joignabilite(0.0, 31, "email")
        score_30j = score_joignabilite(0.0, 30, "email")
        assert score_31j - score_30j == 2.0

    def test_seuil_inactivite_exactement_30j_pas_de_bonus(self):
        # 30 jours = pas de bonus (condition stricte > 30)
        assert score_joignabilite(0.0, 30, "email") == 1.0

    def test_telephone_vaut_1_de_plus_que_email(self):
        delta = score_joignabilite(0.5, 0, "telephone") - score_joignabilite(0.5, 0, "email")
        assert delta == 1.0

    def test_telephone_vaut_1_de_plus_que_sms(self):
        delta = score_joignabilite(0.0, 0, "telephone") - score_joignabilite(0.0, 0, "sms")
        assert delta == 1.0

    @pytest.mark.parametrize("taux", [0.0, 0.25, 0.5, 0.75, 1.0])
    def test_contribution_taux_proportionnelle(self, taux):
        # email, contact récent → score = taux*6 + 1
        score = score_joignabilite(taux, 0, "email")
        assert abs(score - (taux * 6.0 + 1.0)) < 1e-9

    def test_inactivite_et_telephone(self):
        # taux=0, 60j inactif, téléphone → 0 + 2 + 2 = 4
        assert score_joignabilite(0.0, 60, "telephone") == 4.0


# =============================================================================
# SCORE DE VALEUR
# =============================================================================

class TestScoreValeur:

    def test_grand_compte_nps_eleve(self):
        # 300k/100k×5 + 60k/50k×3 + 1 (NPS≥8) = 15 + 3.6 + 1 = 19.6
        score = score_valeur(300_000, 60_000, 9.0, 0)
        assert abs(score - 19.6) < 1e-9

    def test_bonus_nps_declenche_a_8(self):
        avec = score_valeur(100_000, 0, 8.0, 0)
        sans = score_valeur(100_000, 0, 7.9, 0)
        assert avec - sans == 1.0

    def test_bonus_nps_non_declenche_sous_8(self):
        score = score_valeur(0, 0, 7.99, 0)
        assert score == 0.0

    def test_malus_reclamation(self):
        propre = score_valeur(100_000, 0, 5.0, 0)
        deux_reclamations = score_valeur(100_000, 0, 5.0, 2)
        assert abs(propre - deux_reclamations - 1.0) < 1e-9

    def test_score_jamais_negatif(self):
        # Très petit contrat, beaucoup de réclamations
        assert score_valeur(1_000, 0, 1.0, 100) == 0.0

    def test_entrees_nulles(self):
        assert score_valeur(0, 0, 0, 0) == 0.0

    def test_upsell_contribue_proportionnellement(self):
        # 50k upsell → 50k/50k×3 = 3
        score = score_valeur(0, 50_000, 0, 0)
        assert abs(score - 3.0) < 1e-9

    def test_tpe_valeur_minimale(self):
        # 5k/100k×5 = 0.25
        score = score_valeur(5_000, 0, 5.0, 0)
        assert abs(score - 0.25) < 1e-9


# =============================================================================
# SCORE SIGNAL MARKETING
# =============================================================================

class TestScoreSignalMarketing:

    def test_signal_seul(self):
        assert score_signal_marketing(4, False, False, False, 0.0) == 4.0

    def test_tous_les_bonus(self):
        # 4 + 1 + 1 + 1 + 1.0×2 = 9
        assert score_signal_marketing(4, True, True, True, 1.0) == 9.0

    def test_ml_propension_poids_2(self):
        score = score_signal_marketing(0, False, False, False, 0.8)
        assert abs(score - 1.6) < 1e-9

    def test_offre_trimestrielle_ajoute_1(self):
        avec = score_signal_marketing(0, True, False, False, 0.0)
        sans = score_signal_marketing(0, False, False, False, 0.0)
        assert avec - sans == 1.0

    def test_campagne_ajoute_1(self):
        avec = score_signal_marketing(0, False, True, False, 0.0)
        sans = score_signal_marketing(0, False, False, False, 0.0)
        assert avec - sans == 1.0

    def test_saisonnalite_ajoute_1(self):
        avec = score_signal_marketing(0, False, False, True, 0.0)
        sans = score_signal_marketing(0, False, False, False, 0.0)
        assert avec - sans == 1.0

    def test_ml_zero_pas_de_contribution(self):
        assert score_signal_marketing(3, False, False, False, 0.0) == 3.0


# =============================================================================
# SCORE PRIORITÉ (composite)
# =============================================================================

class TestScorePriorite:

    def test_poids_somment_a_1(self):
        # Si tous les sous-scores = 1, résultat = 1
        assert abs(score_priorite(1, 1, 1, 1) - 1.0) < 1e-9

    def test_entrees_nulles(self):
        assert score_priorite(0, 0, 0, 0) == 0.0

    def test_poids_urgence_35pct(self):
        assert abs(score_priorite(10, 0, 0, 0) - 3.5) < 1e-9

    def test_poids_joignabilite_30pct(self):
        assert abs(score_priorite(0, 10, 0, 0) - 3.0) < 1e-9

    def test_poids_valeur_20pct(self):
        assert abs(score_priorite(0, 0, 10, 0) - 2.0) < 1e-9

    def test_poids_marketing_15pct(self):
        assert abs(score_priorite(0, 0, 0, 10) - 1.5) < 1e-9

    def test_exemple_docs_cli0042(self):
        # CLI-0042 : urgence=16, joignabilité=7.2, valeur=4.8, marketing=6.5
        score = score_priorite(16, 7.2, 4.8, 6.5)
        expected = 16 * 0.35 + 7.2 * 0.30 + 4.8 * 0.20 + 6.5 * 0.15
        assert abs(score - expected) < 1e-9

    def test_exemple_docs_cli0187(self):
        # CLI-0187 : urgence=4, joignabilité=5.0, valeur=2.1, marketing=3.0
        score = score_priorite(4, 5.0, 2.1, 3.0)
        expected = 4 * 0.35 + 5.0 * 0.30 + 2.1 * 0.20 + 3.0 * 0.15
        assert abs(score - expected) < 1e-9

    def test_urgence_predomine_sur_autres(self):
        # Un client J-15 sans auto (urgence=16) doit dominer
        score_urgent = score_priorite(16, 0, 0, 0)
        score_bon_tout = score_priorite(0, 8, 8, 8)
        assert score_urgent > score_bon_tout
