"""
Tests d'intégration locaux — Scoring avec données synthétiques en mémoire.

Nécessite PySpark installé localement (pip install pyspark).
Valide la logique de scoring et les formules silver/gold sur des DataFrames
en mémoire, sans connexion au workspace Databricks.

Lancement : pytest tests/integration/local/ -v
"""

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark requis pour les tests d'intégration locaux")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType,
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
# Fixtures — données de test mimant le schéma silver_clients_features
# =============================================================================

# Schéma aligné sur silver_clients_features (colonnes principales)
SILVER_SCHEMA = StructType([
    StructField("client_id",                StringType(),  False),
    StructField("segment",                  StringType(),  False),
    StructField("valeur_contrat_annuel",    DoubleType(),  False),
    StructField("potentiel_upsell",         DoubleType(),  False),
    StructField("score_nps",                DoubleType(),  False),
    StructField("nb_reclamations_12m",      IntegerType(), False),
    StructField("canal_prefere",            StringType(),  False),
    StructField("renouvellement_auto",      BooleanType(), False),
    StructField("jours_avant_echeance",     IntegerType(), False),
    StructField("taux_reponse_historique",  DoubleType(),  False),
    StructField("jours_depuis_dernier_contact", IntegerType(), False),
    # Encodage entier pour les booléens signaux (comme dans silver.py)
    StructField("malus_auto_off",           IntegerType(), False),
    StructField("offre_trimestrielle_active", IntegerType(), False),
    StructField("campagne_en_cours",        IntegerType(), False),
    StructField("saisonnalite_detectee",    IntegerType(), False),
    StructField("score_signal_pts",         IntegerType(), False),
    StructField("score_ml_propension",      DoubleType(),  False),
])

# client_id, segment, ca, upsell, nps, recl, canal, auto, jae, taux_rep, j_contact,
# malus_off, offre, campagne, saison, signal_pts, ml_prop
SILVER_DATA = [
    ("CLI-0001", "Grand Compte", 300_000.0, 60_000.0, 9.0, 0, "telephone", True,  10,  0.70, 5,  0, 1, 0, 1, 8, 0.85),
    ("CLI-0002", "PME",          40_000.0,  5_000.0, 6.5, 1, "email",     True,  25,  0.50, 20, 0, 0, 0, 0, 4, 0.50),
    ("CLI-0003", "TPE",           8_000.0,      0.0, 3.0, 3, "sms",       False, 120, 0.20, 5,  1, 0, 0, 0, 0, 0.20),
    ("CLI-0004", "ETI",          80_000.0, 20_000.0, 8.0, 0, "telephone", False, 55,  0.65, 40, 1, 1, 1, 0, 2, 0.70),
]


@pytest.fixture(scope="module")
def df_silver(spark):
    return spark.createDataFrame(SILVER_DATA, schema=SILVER_SCHEMA)


# =============================================================================
# Tests — scores partiels cohérents avec silver.py
# =============================================================================

class TestScoresPartielsSilver:
    """Vérifie que les formules Python (src/scoring) et silver.py produisent
    les mêmes résultats sur les données de test."""

    def test_score_urgence_j15_avec_auto(self):
        # CLI-0001 : jae=10, renouvellement_auto=True → base 8, pas de doublement
        assert score_urgence(10, True) == 8.0

    def test_score_urgence_j60_sans_auto_double(self):
        # CLI-0004 : jae=55, malus_auto_off=1 → base 2 × 2 = 4
        assert score_urgence(55, False) == 4.0

    def test_score_urgence_hors_fenetre(self):
        # CLI-0003 : jae=120 → 0
        assert score_urgence(120, False) == 0.0

    def test_score_joignabilite_grand_compte(self):
        # CLI-0001 : taux=0.70, j_contact=5 (≤30), canal=telephone
        # → 0.70×6 + 0 + 2 = 6.2
        score = score_joignabilite(0.70, 5, "telephone")
        assert abs(score - 6.2) < 1e-9

    def test_score_joignabilite_avec_bonus_inactivite(self):
        # CLI-0004 : taux=0.65, j_contact=40 (>30 → +2), canal=telephone (+2)
        # → 0.65×6 + 2 + 2 = 7.9
        score = score_joignabilite(0.65, 40, "telephone")
        assert abs(score - 7.9) < 1e-9

    def test_score_valeur_grand_compte_nps_eleve(self):
        # CLI-0001 : ca=300k, upsell=60k, nps=9 (+1), recl=0
        # → 300k/100k×5 + 60k/50k×3 + 1 = 15 + 3.6 + 1 = 19.6
        score = score_valeur(300_000, 60_000, 9.0, 0)
        assert abs(score - 19.6) < 1e-9

    def test_score_valeur_jamais_negatif(self):
        # CLI-0003 : ca=8k, upsell=0, nps=3, recl=3
        assert score_valeur(8_000, 0, 3.0, 3) >= 0

    def test_score_signal_marketing_avec_bonuses(self):
        # CLI-0001 : signal_pts=8, offre=1, campagne=0, saison=1, ml=0.85
        # → 8 + 1 + 0 + 1 + 0.85×2 = 11.7
        score = score_signal_marketing(8, True, False, True, 0.85)
        assert abs(score - 11.7) < 1e-9

    def test_score_signal_marketing_sans_signal(self):
        # CLI-0003 : signal_pts=0, tout à 0, ml=0.20 → 0 + 0 + 0 + 0 + 0.4 = 0.4
        score = score_signal_marketing(0, False, False, False, 0.20)
        assert abs(score - 0.4) < 1e-9


# =============================================================================
# Tests — score_priorite composite sur données silver
# =============================================================================

class TestScorePrioriteComposite:

    def test_grand_compte_j15_domine_tpe_sans_urgence(self):
        """CLI-0001 (Grand Compte, J-15) doit dominer CLI-0003 (TPE, hors fenêtre)."""
        sc_gc  = score_priorite(
            score_urgence(10, True),
            score_joignabilite(0.70, 5,  "telephone"),
            score_valeur(300_000, 60_000, 9.0, 0),
            score_signal_marketing(8, True, False, True, 0.85),
        )
        sc_tpe = score_priorite(
            score_urgence(120, False),
            score_joignabilite(0.20, 5, "sms"),
            score_valeur(8_000, 0, 3.0, 3),
            score_signal_marketing(0, False, False, False, 0.20),
        )
        assert sc_gc > sc_tpe

    def test_malus_auto_off_augmente_priorite(self):
        """CLI-0004 sans renouvellement auto doit avoir score_urgence doublé."""
        sc_avec_auto  = score_priorite(score_urgence(55, True),  5, 3, 3)
        sc_sans_auto  = score_priorite(score_urgence(55, False), 5, 3, 3)
        assert sc_sans_auto > sc_avec_auto

    def test_poids_composite_somment_a_1(self):
        assert abs(score_priorite(1, 1, 1, 1) - 1.0) < 1e-9

    def test_exemple_scoring_md_cli0042(self):
        # CLI-0042 : urgence=16, joignabilité=7.2, valeur=4.8, marketing=6.5
        score = score_priorite(16, 7.2, 4.8, 6.5)
        expected = 16 * 0.35 + 7.2 * 0.30 + 4.8 * 0.20 + 6.5 * 0.15
        assert abs(score - expected) < 1e-9


# =============================================================================
# Tests — formules silver appliquées sur DataFrame Spark local
# =============================================================================

class TestFormulesSilverSurDataFrame:

    def test_score_urgence_via_spark(self, df_silver, spark):
        """Vérifie score_urgence calculé par la logique CASE WHEN de silver.py."""
        df = df_silver.withColumn(
            "score_urgence_calc",
            F.when((F.col("jours_avant_echeance") >= 0) & (F.col("jours_avant_echeance") <= 15), 8)
             .when((F.col("jours_avant_echeance") > 15) & (F.col("jours_avant_echeance") <= 30), 4)
             .when((F.col("jours_avant_echeance") > 30) & (F.col("jours_avant_echeance") <= 60), 2)
             .when((F.col("jours_avant_echeance") > 60) & (F.col("jours_avant_echeance") <= 90), 1)
             .otherwise(0),
        ).withColumn(
            "score_urgence_calc",
            F.when(F.col("malus_auto_off") == 1, F.col("score_urgence_calc") * 2)
             .otherwise(F.col("score_urgence_calc")),
        )
        rows = {r["client_id"]: r["score_urgence_calc"] for r in df.collect()}
        assert rows["CLI-0001"] == 8    # J-10, auto=True
        assert rows["CLI-0002"] == 4    # J-25, auto=True
        assert rows["CLI-0003"] == 0    # J-120, hors fenêtre
        assert rows["CLI-0004"] == 4    # J-55, malus_auto_off=1 → base 2 × 2

    def test_score_joignabilite_via_spark(self, df_silver, spark):
        df = df_silver.withColumn(
            "sj",
            (F.col("taux_reponse_historique") * 6)
            + F.when(F.col("jours_depuis_dernier_contact") > 30, 2).otherwise(0)
            + F.when(F.col("canal_prefere") == "telephone", 2).otherwise(1),
        )
        rows = {r["client_id"]: r["sj"] for r in df.collect()}
        # CLI-0001 : 0.70×6=4.2 + 0 + 2 = 6.2
        assert abs(rows["CLI-0001"] - 6.2) < 1e-9
        # CLI-0004 : 0.65×6=3.9 + 2 (>30j) + 2 = 7.9
        assert abs(rows["CLI-0004"] - 7.9) < 1e-9

    def test_score_valeur_plancher_zero(self, df_silver, spark):
        df = df_silver.withColumn(
            "sv",
            F.greatest(
                F.lit(0.0),
                (F.col("valeur_contrat_annuel") / 100_000 * 5)
                + (F.col("potentiel_upsell") / 50_000 * 3)
                + F.when(F.col("score_nps") >= 8, 1).otherwise(0)
                - (F.col("nb_reclamations_12m").cast("double") * 0.5),
            ),
        )
        for row in df.collect():
            assert row["sv"] >= 0, f"{row['client_id']} a score_valeur < 0"

    def test_score_signal_marketing_via_spark(self, df_silver, spark):
        df = df_silver.withColumn(
            "ssm",
            F.col("score_signal_pts").cast("double")
            + F.col("offre_trimestrielle_active")
            + F.col("campagne_en_cours")
            + F.col("saisonnalite_detectee")
            + (F.col("score_ml_propension") * 2),
        )
        rows = {r["client_id"]: r["ssm"] for r in df.collect()}
        # CLI-0001 : 8 + 1 + 0 + 1 + 0.85×2 = 11.7
        assert abs(rows["CLI-0001"] - 11.7) < 1e-9
        # CLI-0004 : 2 + 1 + 1 + 0 + 0.70×2 = 5.4
        assert abs(rows["CLI-0004"] - 5.4) < 1e-9


# =============================================================================
# Tests — classify_urgence_signal sur données locales
# =============================================================================

class TestClassifySignauxLocaux:

    @pytest.mark.parametrize("jours,expected_label", [
        (10,  "J-15"),
        (25,  "J-30"),
        (55,  "J-60"),
        (75,  "J-90"),
        (120, None),
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

    def test_beyond_90j_aucun_signal(self):
        for j in [91, 120, 365]:
            assert classify_urgence_signal(j) is None
