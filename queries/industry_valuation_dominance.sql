-- Apakah industri lebih dominan dalam menjelaskan valuasi?
-- Query ini mencari industri dengan rata-rata dan total valuasi terbesar.
-- Hasilnya dipakai sebagai pembanding terhadap pengaruh tier universitas.

SELECT
    c.industry,
    COUNT(*) AS executive_company_pairs,
    COUNT(DISTINCT f.company_name) AS total_companies,
    ROUND(SUM(f.valuation_usd) / 1000000000, 2) AS total_valuation_billion_usd,
    ROUND(AVG(f.valuation_usd) / 1000000000, 2) AS avg_valuation_billion_usd,
    ROUND(MEDIAN(f.valuation_usd) / 1000000000, 2) AS median_valuation_billion_usd,
    ROUND(AVG(f.experience_grit_index), 2) AS avg_grit_index
FROM read_parquet('s3://gold/fact_valuation_grit.parquet') f
JOIN read_parquet('s3://gold/dim_company.parquet') c
    ON f.company_name = c.company_name
WHERE c.industry IS NOT NULL
  AND f.valuation_usd > 0
GROUP BY c.industry
HAVING COUNT(DISTINCT f.company_name) >= 2
ORDER BY avg_valuation_billion_usd DESC
LIMIT 10;
