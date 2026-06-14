"""
Tests d'intégration locaux — Scoring avec données synthétiques en mémoire.

Ces tests nécessitent PySpark installé localement (pip install pyspark).
Ils valident la logique de scoring appliquée à un DataFrame Spark minimal,
sans connexion au workspace Databricks.

Lancement : pytest tests/integration/local/ -v
"""

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark requis pour les tests d'intégration locaux")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)
from src.scoring.scoring import (
    score_urgence, score_joignabilite, score_valeur,
    score_signal_marketing, score_priorite, classify_urgence_signal,
)


# =============================================================================
# Session Spark locale (partagée par tous les tests du module)
# =============================================================================

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("fidelisation-integration-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# =============================================================================
# Fixtures — données de test
# =============================================================================

CLIENTS_SCHEMA = StructType([
    StructField("client_id",             StringType(),  False),
    StructField("segment",               StringType(),  False),
    StructField("valeur_contrat_annuel", DoubleType(),  False),
    StructField("potentiel_upsell",      DoubleType(),  False),
    StructField("score_nps",             DoubleType(),  False),
    StructField("nb_reclamations_12m",   IntegerType(), False),
    StructField("canal_prefere",         StringType(),  False),
    StructField("renouvellement_auto",   BooleanType(), False),
])

CLIENTS_DATA = [
    # client_id,     segment,       ca,       upsell,  nps,  recl, canal,        auto
    ("CLI-0001", "Grand Compte", 300_000.0, 60_000.0, 9.0,  0,  "telephone", True),
    ("CLI-0002", "PME",          40_000.0,  5_000.0,  6.5,  1,  "email",     True),
    ("CLI-0003", "TPE",           8_000.0,      0.0,  3.0,  3,  "sms",       False),
    ("CLI-0004", "ETI",          80_000.0, 20_000.0,  8.0,  0,  "telephone", False),
]

SIGNAUX_DATA = [
    # client_id,  jours_avant_fin, auto,  offre, campagne, saison, ml_prop, signal_pts
    ("CLI-0001",  10, True,  True,  False, True,  0.85, 8),   # J-15
    ("CLI-0002",  25, True,  False, False, False, 0.50, 4),   # J-30
    ("CLI-0003", 120, False, False, False, False, 0.20, 2),   # hors fenêtre (silence)
    ("CLI-0004",  55, False, True,  True,  False, 0.70, 2),   # J-60 sans auto → ×2
]


@pytest.fixture(scope="module")
def df_clients(spark):
    return spark.createDataFrame(CLIENTS_DATA, schema=CLIENTS_SCHEMA)


# =============================================================================
# Tests — Fonctions de scoring appliquées sur DataFrame
# =============================================================================

class TestScoringFunctionsOnDataFrame:

    def test_score_valeur_grand_compte(self, df_clients):
        row = df_clients.filter(F.col("client_id") == "CLI-0001").collect()[0]
        score = score_valeur(
            row["valeur_contrat_annuel"],
            row["potentiel_upsell"],
            row["score_nps"],
            row["nb_reclamations_12m"],
        )
        expected = (300_000 / 100_000 * 5) + (60_000 / 50_000 * 3) + 1  # NPS≥8
        assert abs(score - expected) < 1e-9

    def test_score_valeur_tpe_avec_reclamations(self, df_clients):
        row = df_clients.filter(F.col("client_id") == "CLI-0003").collect()[0]
        score = score_valeur(
            row["valeur_contrat_annuel"],
            row["potentiel_upsell"],
            row["score_nps"],
            row["nb_reclamations_12m"],
        )
        assert score >= 0  # jamais négatif

    def test_score_urgence_j15_avec_auto(self):
        assert score_urgence(10, True) == 8.0

    def test_score_urgence_j60_sans_auto_double(self):
        assert score_urgence(55, False) == 4.0  # base=2, ×2

    def test_grand_compte_score_priorite_eleve(self, df_clients):
        row = df_clients.filter(F.col("client_id") == "CLI-0001").collect()[0]
        sc_valeur = score_valeur(
            row["valeur_contrat_annuel"], row["potentiel_upsell"],
            row["score_nps"], row["nb_reclamations_12m"]
        )
        sc_urgence = score_urgence(10, row["renouvellement_auto"])
        sc_joign   = score_joignabilite(0.7, 5, row["canal_prefere"])
        sc_mktg    = score_signal_marketing(8, True, False, True, 0.85)
        score      = score_priorite(sc_urgence, sc_joign, sc_valeur, sc_mktg)
        assert score > 5.0  # Grand Compte J-15 doit être prioritaire

    def test_tpe_score_priorite_faible(self, df_clients):
        row = df_clients.filter(F.col("client_id") == "CLI-0003").collect()[0]
        sc_valeur = score_valeur(
            row["valeur_contrat_annuel"], row["potentiel_upsell"],
            row["score_nps"], row["nb_reclamations_12m"]
        )
        sc_urgence = 0  # hors fenêtre 90j
        sc_joign   = score_joignabilite(0.2, 5, row["canal_prefere"])
        sc_mktg    = score_signal_marketing(2, False, False, False, 0.20)
        score      = score_priorite(sc_urgence, sc_joign, sc_valeur, sc_mktg)
        assert score < 5.0


# =============================================================================
# Tests — classify_urgence_signal avec données synthétiques
# =============================================================================

class TestClassifySignauxLocaux:

    @pytest.mark.parametrize("jours,expected_label", [
        (10,  "J-15"),
        (25,  "J-30"),
        (55,  "J-60"),
        (75,  "J-90"),
        (120, None),   # hors fenêtre
    ])
    def test_classification_signaux(self, jours, expected_label):
        result = classify_urgence_signal(jours)
        if expected_label is None:
            assert result is None
        else:
            assert result[0] == expected_label

    def test_signal_j15_max_pts(self):
        _, pts = classify_urgence_signal(0)
        assert pts == 8

    def test_signal_absent_au_dela_90j(self):
        for jours in [91, 120, 180, 365]:
            assert classify_urgence_signal(jours) is None


# =============================================================================
# Tests — Vecteur complet de scoring sur données en mémoire
# =============================================================================

class TestScoringPipelineComplet:

    def test_grand_compte_domine_tpe(self, df_clients):
        """Le Grand Compte J-15 doit avoir un score composite supérieur au TPE sans urgence."""
        def compute(row, jours, signal_pts, offre, ml):
            sc_u = score_urgence(jours, row["renouvellement_auto"])
            sc_j = score_joignabilite(0.6, 10, row["canal_prefere"])
            sc_v = score_valeur(
                row["valeur_contrat_annuel"], row["potentiel_upsell"],
                row["score_nps"], row["nb_reclamations_12m"]
            )
            sc_m = score_signal_marketing(signal_pts, offre, False, False, ml)
            return score_priorite(sc_u, sc_j, sc_v, sc_m)

        gc  = df_clients.filter(F.col("client_id") == "CLI-0001").collect()[0]
        tpe = df_clients.filter(F.col("client_id") == "CLI-0003").collect()[0]

        score_gc  = compute(gc,  jours=10,  signal_pts=8, offre=True,  ml=0.85)
        score_tpe = compute(tpe, jours=120, signal_pts=2, offre=False, ml=0.20)

        assert score_gc > score_tpe

    def test_sans_auto_augmente_score_urgence(self):
        """Un client sans renouvellement auto doit obtenir un score urgence plus élevé."""
        score_avec_auto  = score_priorite(score_urgence(20, True),  5, 3, 3)
        score_sans_auto  = score_priorite(score_urgence(20, False), 5, 3, 3)
        assert score_sans_auto > score_avec_auto
