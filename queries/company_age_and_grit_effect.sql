-- Apakah usia perusahaan dan grit index lebih menjelaskan valuasi?
-- Query ini mengelompokkan perusahaan berdasarkan usia dan membandingkan valuasi
-- serta grit index. Ini menjawab apakah faktor non-universitas ikut dominan.

SELECT
    CASE
        WHEN f.company_age_years <= 5 THEN '0-5 Years'
        WHEN f.company_age_years <= 10 THEN '6-10 Years'
        WHEN f.company_age_years <= 20 THEN '11-20 Years'
        ELSE '20+ Years'
    END AS company_age_bucket,
    COUNT(*) AS executive_company_pairs,
    COUNT(DISTINCT f.company_name) AS total_companies,
    ROUND(AVG(f.company_age_years), 2) AS avg_company_age_years,
    ROUND(AVG(f.experience_grit_index), 2) AS avg_grit_index,
    ROUND(SUM(f.valuation_usd) / 1000000000, 2) AS total_valuation_billion_usd,
    ROUND(AVG(f.valuation_usd) / 1000000000, 2) AS avg_valuation_billion_usd,
    ROUND(MEDIAN(f.valuation_usd) / 1000000000, 2) AS median_valuation_billion_usd
FROM read_parquet('s3://gold/fact_valuation_grit.parquet') f
WHERE f.company_age_years IS NOT NULL
  AND f.experience_grit_index IS NOT NULL
  AND f.valuation_usd > 0
GROUP BY company_age_bucket
ORDER BY avg_valuation_billion_usd DESC;
