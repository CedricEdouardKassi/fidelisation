-- =============================================================================
-- COUCHE GOLD — Scoring composite, priorisation et KPIs journaliers
-- =============================================================================
-- VUE MATÉRIALISÉE 1 : gold_priorites_clients  — liste scorée par commercial et région
-- VUE MATÉRIALISÉE 2 : gold_dashboard_jour     — KPIs agrégés pour le dashboard
--
-- Formule composite (docs/scoring.md) :
--   score_priorite = urgence×0.35 + joignabilite×0.30 + valeur×0.20 + marketing×0.15
-- =============================================================================


-- -----------------------------------------------------------------------------
-- gold_priorites_clients
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_priorites_clients
COMMENT "Liste quotidienne scorée et priorisée des clients à contacter"
AS

WITH

-- Score composite + nom du commercial
scored AS (
  SELECT
    s.*,
    ROUND(
        (s.score_urgence            * 0.35)
      + (s.score_joignabilite       * 0.30)
      + (s.score_valeur             * 0.20)
      + (s.score_signal_marketing   * 0.15),
      4
    )                              AS score_priorite,
    CONCAT(c.prenom, ' ', c.nom)   AS commercial_nom
  FROM LIVE.silver_clients_features s
  LEFT JOIN LIVE.bronze_commerciaux c
    ON s.commercial_id = c.commercial_id
),

-- Rang au sein de chaque région (RANK gère les ex-æquo)
ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY region
      ORDER BY score_priorite DESC
    ) AS rang_priorite_region
  FROM scored
),

-- Meilleur créneau disponible par commercial : agenda × heatmap joignabilité
best_slots AS (
  SELECT
    a.commercial_id,
    a.date_slot                                                     AS meilleur_jour_appel,
    CONCAT(LPAD(CAST(a.heure_slot AS STRING), 2, '0'), 'h00')      AS heure_recommandee,
    ROW_NUMBER() OVER (
      PARTITION BY a.commercial_id
      ORDER BY h.taux_reponse_moyen DESC, a.date_slot ASC, a.heure_slot ASC
    ) AS rn
  FROM LIVE.bronze_agenda a
  JOIN LIVE.bronze_heatmap h
    ON  a.jour_semaine = h.jour_semaine
    AND a.heure_slot   = h.heure_slot
  WHERE a.disponible = TRUE
    AND a.date_slot  >= CURRENT_DATE()
)

SELECT
  r.*,
  (r.rang_priorite_region <= 20)    AS a_appeler_aujourdhui,
  t.meilleur_jour_appel,
  t.heure_recommandee
FROM ranked r
LEFT JOIN (
  SELECT commercial_id, meilleur_jour_appel, heure_recommandee
  FROM   best_slots
  WHERE  rn = 1
) t ON r.commercial_id = t.commercial_id;


-- -----------------------------------------------------------------------------
-- gold_dashboard_jour  — 1 ligne de KPIs agrégés pour le dashboard
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_dashboard_jour
COMMENT "KPIs agrégés du moteur de fidélisation pour le dashboard journalier"
AS
SELECT
  COUNT(CASE WHEN a_appeler_aujourdhui THEN 1 END)                                    AS nb_clients_a_appeler,
  COUNT(CASE WHEN jours_avant_echeance BETWEEN 0 AND 30 THEN 1 END)                  AS nb_contrats_j30,
  ROUND(AVG(taux_reponse_historique) * 100, 1)                                        AS taux_joignabilite_moyen,
  ROUND(SUM(CASE WHEN a_appeler_aujourdhui THEN valeur_contrat_annuel ELSE 0 END), 2) AS ca_potentiel_en_jeu
FROM LIVE.gold_priorites_clients;
