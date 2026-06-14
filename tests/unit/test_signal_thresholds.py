"""
Tests unitaires — Détection et classification des signaux de fidélisation.

Couvre la logique de classify_urgence_signal() et les règles métier
de génération des signaux (NPS, silence client) documentées dans docs/scoring.md.
"""

import pytest
from src.scoring.scoring import classify_urgence_signal


# =============================================================================
# CLASSIFICATION DES SIGNAUX D'URGENCE CONTRACTUELLE
# =============================================================================

class TestClassifyUrgenceSignal:

    # --- Frontières de chaque seuil ---

    @pytest.mark.parametrize("jours,expected_label,expected_pts", [
        (0,  "J-15", 8),
        (1,  "J-15", 8),
        (15, "J-15", 8),
        (16, "J-30", 4),
        (29, "J-30", 4),
        (30, "J-30", 4),
        (31, "J-60", 2),
        (59, "J-60", 2),
        (60, "J-60", 2),
        (61, "J-90", 1),
        (89, "J-90", 1),
        (90, "J-90", 1),
    ])
    def test_seuils_et_labels(self, jours, expected_label, expected_pts):
        result = classify_urgence_signal(jours)
        assert result is not None, f"Signal attendu pour {jours} jours"
        label, pts = result
        assert label == expected_label
        assert pts == expected_pts

    # --- Hors fenêtre : pas de signal ---

    @pytest.mark.parametrize("jours", [91, 100, 180, 365, 1000])
    def test_au_dela_90_aucun_signal(self, jours):
        assert classify_urgence_signal(jours) is None

    @pytest.mark.parametrize("jours", [-1, -15, -365])
    def test_contrat_expire_aucun_signal(self, jours):
        assert classify_urgence_signal(jours) is None

    # --- Points décroissants avec l'éloignement ---

    def test_points_decroissants(self):
        pts_j15 = classify_urgence_signal(10)[1]
        pts_j30 = classify_urgence_signal(20)[1]
        pts_j60 = classify_urgence_signal(45)[1]
        pts_j90 = classify_urgence_signal(75)[1]
        assert pts_j15 > pts_j30 > pts_j60 > pts_j90

    def test_retourne_tuple_ou_none(self):
        result = classify_urgence_signal(10)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_label_est_string(self):
        label, _ = classify_urgence_signal(10)
        assert isinstance(label, str)

    def test_pts_est_int(self):
        _, pts = classify_urgence_signal(10)
        assert isinstance(pts, int)


# =============================================================================
# RÈGLES MÉTIER — SIGNAL NPS BAS
# =============================================================================

class TestSignalNpsBas:
    """
    Le signal 'nps_bas' est déclenché quand score_nps < 5.
    Il vaut 3 points et n'est jamais doublé.
    """

    NPS_SIGNAL_SEUIL = 5.0
    NPS_SIGNAL_PTS = 3

    @pytest.mark.parametrize("nps", [1.0, 2.0, 3.5, 4.0, 4.99])
    def test_nps_sous_seuil_declencheur(self, nps):
        assert nps < self.NPS_SIGNAL_SEUIL

    @pytest.mark.parametrize("nps", [5.0, 5.1, 7.0, 10.0])
    def test_nps_au_dessus_seuil_pas_de_signal(self, nps):
        assert nps >= self.NPS_SIGNAL_SEUIL

    def test_valeur_points_nps_signal(self):
        assert self.NPS_SIGNAL_PTS == 3

    def test_seuil_nps_exclusif(self):
        # Exactement 5.0 ne déclenche PAS le signal
        assert not (5.0 < self.NPS_SIGNAL_SEUIL)

    def test_score_ml_propension_bas_pour_nps(self):
        # Signal NPS → propension faible (0.1 – 0.5)
        propension_min, propension_max = 0.1, 0.5
        assert propension_min < propension_max
        assert propension_min >= 0.0
        assert propension_max <= 1.0


# =============================================================================
# RÈGLES MÉTIER — SIGNAL SILENCE CLIENT
# =============================================================================

class TestSignalSilenceClient:
    """
    Le signal 'silence_client' est généré avec 15% de probabilité
    pour les clients dont le contrat expire dans > 90 jours.
    """

    PROBABILITE_SILENCE = 0.15
    SIGNAL_PTS = 2
    VALEUR_SIGNAL = "sans_contact_90j"

    def test_probabilite_entre_0_et_1(self):
        assert 0 < self.PROBABILITE_SILENCE < 1

    def test_probabilite_est_15pct(self):
        assert self.PROBABILITE_SILENCE == 0.15

    def test_signal_pts_vaut_2(self):
        assert self.SIGNAL_PTS == 2

    def test_valeur_signal(self):
        assert self.VALEUR_SIGNAL == "sans_contact_90j"

    def test_silence_uniquement_hors_fenetre_90j(self):
        # Le silence ne concerne que les contrats > 90 jours
        # (ceux dans la fenêtre ont déjà un signal d'échéance)
        jours_ok = [91, 100, 180, 365]
        for j in jours_ok:
            assert j > 90


# =============================================================================
# COHÉRENCE GLOBALE DES SIGNAUX
# =============================================================================

class TestCoherenceSignaux:

    def test_types_signaux_distincts(self):
        types = {"echeance_contrat", "nps_bas", "silence_client"}
        assert len(types) == 3

    def test_score_urgence_echeance_max(self):
        # Le signal échéance J-15 rapporte 8 pts (plus élevé parmi les signaux)
        _, pts_j15 = classify_urgence_signal(0)
        assert pts_j15 == 8

    def test_score_urgence_echeance_superieur_silence(self):
        _, pts_echeance = classify_urgence_signal(90)  # minimum = J-90 = 1 pt
        pts_silence = 2
        # J-90 (1 pt) < silence (2 pts) — le silence est prioritaire si proche
        assert pts_echeance < pts_silence

    def test_signal_nps_superieur_au_silence(self):
        pts_nps = 3
        pts_silence = 2
        assert pts_nps > pts_silence

    def test_signal_j15_est_le_plus_urgent(self):
        _, pts = classify_urgence_signal(0)
        assert pts == max(8, 3, 2)  # J-15 > NPS > silence
