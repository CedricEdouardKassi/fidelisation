# =============================================================================
# COUCHE SILVER — Features enrichies par client pour le scoring de fidélisation
# =============================================================================
# API moderne SDP : from pyspark import pipelines as dp
#
# Jointure clients × appels × signaux.
# Calcul des 4 scores partiels documentés dans docs/scoring.md :
#   score_urgence | score_joignabilite | score_valeur | score_signal_marketing
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_clients_features",
    comment="Features enrichies par client : agrégats appels, signaux et scores partiels",
)
def silver_clients_features():
    clients = spark.read.table("LIVE.bronze_clients")
    appels  = spark.read.table("LIVE.bronze_appels")
    signaux = spark.read.table("LIVE.bronze_signaux")

    today = F.current_date()

    # ── 1. Agrégation historique d'appels (12 mois glissants) ────────────────
    appels_agg = (
        appels
        .withColumn("date_appel_dt", F.to_date("date_appel"))
        .filter(F.col("date_appel_dt") >= F.date_sub(today, 365))
        .groupBy("client_id")
        .agg(
            F.count("appel_id").alias("nb_appels_12m"),
            F.sum(F.when(F.col("resultat") == "décroché", 1).otherwise(0))
             .alias("nb_decroches_12m"),
            F.sum(F.when(F.col("direction") == "sortant", 1).otherwise(0))
             .alias("nb_appels_sortants_12m"),
            F.max(F.to_date("date_appel")).alias("date_dernier_contact"),
        )
        .withColumn(
            "taux_reponse_historique",
            F.round(
                F.col("nb_decroches_12m")
                / F.when(F.col("nb_appels_12m") == 0, 1).otherwise(F.col("nb_appels_12m")),
                3,
            ),
        )
        .withColumn(
            "jours_depuis_dernier_contact",
            F.datediff(today, F.col("date_dernier_contact")),
        )
    )

    # ── 2. Signaux : agrégation des booléens et du score le plus élevé ───────
    # Les booléens sont encodés en entiers (0/1) pour compatibilité fillna + SQL
    signaux_agg = (
        signaux
        .groupBy("client_id")
        .agg(
            F.max("score_signal_pts").alias("score_signal_pts"),
            F.max("score_ml_propension").alias("score_ml_propension"),
            F.max(F.when(~F.col("renouvellement_auto"), 1).otherwise(0)).alias("malus_auto_off"),
            F.max(F.when(F.col("offre_trimestrielle_active"), 1).otherwise(0)).alias("offre_trimestrielle_active"),
            F.max(F.when(F.col("campagne_en_cours"),          1).otherwise(0)).alias("campagne_en_cours"),
            F.max(F.when(F.col("saisonnalite_detectee"),      1).otherwise(0)).alias("saisonnalite_detectee"),
        )
    )

    # ── 3. Jointures et valeurs par défaut ────────────────────────────────────
    df = (
        clients
        # jours_avant_echeance recalculé en temps réel (pas repris du signal statique)
        .withColumn(
            "jours_avant_echeance",
            F.datediff(F.to_date("date_fin_contrat"), today),
        )
        .join(appels_agg, "client_id", "left")
        .join(signaux_agg, "client_id", "left")
        .fillna({
            "nb_appels_12m":                0,
            "nb_decroches_12m":             0,
            "nb_appels_sortants_12m":       0,
            "taux_reponse_historique":      0.0,
            "jours_depuis_dernier_contact": 999,
            "score_signal_pts":             0,
            "score_ml_propension":          0.0,
            "malus_auto_off":               0,
            "offre_trimestrielle_active":   0,
            "campagne_en_cours":            0,
            "saisonnalite_detectee":        0,
        })
    )

    # ── 4. Score d'urgence (poids 35%) ───────────────────────────────────────
    # Base selon la fenêtre d'échéance, doublée si renouvellement auto désactivé
    df = (
        df
        .withColumn(
            "score_urgence",
            F.when((F.col("jours_avant_echeance") >= 0) & (F.col("jours_avant_echeance") <= 15), 8)
             .when((F.col("jours_avant_echeance") > 15) & (F.col("jours_avant_echeance") <= 30), 4)
             .when((F.col("jours_avant_echeance") > 30) & (F.col("jours_avant_echeance") <= 60), 2)
             .when((F.col("jours_avant_echeance") > 60) & (F.col("jours_avant_echeance") <= 90), 1)
             .otherwise(0),
        )
        .withColumn(
            "score_urgence",
            F.when(F.col("malus_auto_off") == 1, F.col("score_urgence") * 2)
             .otherwise(F.col("score_urgence")),
        )
    )

    # ── 5. Score de joignabilité (poids 30%) ─────────────────────────────────
    df = df.withColumn(
        "score_joignabilite",
        F.round(
            (F.col("taux_reponse_historique") * 6)
            + F.when(F.col("jours_depuis_dernier_contact") > 30, 2).otherwise(0)
            + F.when(F.col("canal_prefere") == "telephone", 2).otherwise(1),
            2,
        ),
    )

    # ── 6. Score de valeur (poids 20%) ───────────────────────────────────────
    df = df.withColumn(
        "score_valeur",
        F.round(
            F.greatest(
                F.lit(0.0),
                (F.col("valeur_contrat_annuel") / 100_000 * 5)
                + (F.col("potentiel_upsell") / 50_000 * 3)
                + F.when(F.col("score_nps") >= 8, 1).otherwise(0)
                - (F.col("nb_reclamations_12m").cast("double") * 0.5),
            ),
            2,
        ),
    )

    # ── 7. Score signal marketing (poids 15%) ────────────────────────────────
    df = df.withColumn(
        "score_signal_marketing",
        F.round(
            F.col("score_signal_pts")
            + F.col("offre_trimestrielle_active")
            + F.col("campagne_en_cours")
            + F.col("saisonnalite_detectee")
            + (F.col("score_ml_propension") * 2),
            2,
        ),
    )

    return df
