-- Apakah ranking QS yang lebih tinggi berkaitan dengan valuasi lebih tinggi?
-- Catatan interpretasi:
-- QS rank yang lebih kecil berarti universitas lebih tinggi.
-- Korelasi negatif antara qs_world_ranking dan log valuation berarti ranking lebih baik
-- cenderung berkaitan dengan valuasi lebih tinggi.

SELECT
    COUNT(*) AS ranked_observations,
    COUNT(DISTINCT f.company_name) AS ranked_companies,
    ROUND(CORR(e.qs_world_ranking, LN(f.valuation_usd)), 4) AS corr_qs_rank_vs_log_valuation,
    ROUND(CORR(1.0 / e.qs_world_ranking, LN(f.valuation_usd)), 4) AS corr_qs_quality_vs_log_valuation,
    ROUND(AVG(e.qs_world_ranking), 2) AS avg_qs_rank,
    ROUND(AVG(f.valuation_usd) / 1000000000, 2) AS avg_valuation_billion_usd,
    ROUND(MEDIAN(f.valuation_usd) / 1000000000, 2) AS median_valuation_billion_usd
FROM read_parquet('s3://gold/fact_valuation_grit.parquet') f
JOIN read_parquet('s3://gold/dim_executive.parquet') e
    ON f.executive_name = e.executive_name
WHERE e.qs_world_ranking IS NOT NULL
  AND e.qs_world_ranking > 0
  AND f.valuation_usd > 0;
