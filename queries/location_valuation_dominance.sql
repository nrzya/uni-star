-- Apakah lokasi perusahaan lebih dominan dalam menjelaskan valuasi?
-- Query ini membandingkan negara berdasarkan jumlah perusahaan, total valuasi,
-- rata-rata valuasi, median valuasi, dan komposisi industri.

SELECT
    c.country,
    c.continent,
    COUNT(*) AS executive_company_pairs,
    COUNT(DISTINCT f.company_name) AS total_companies,
    COUNT(DISTINCT c.industry) AS industry_diversity,
    ROUND(SUM(f.valuation_usd) / 1000000000, 2) AS total_valuation_billion_usd,
    ROUND(AVG(f.valuation_usd) / 1000000000, 2) AS avg_valuation_billion_usd,
    ROUND(MEDIAN(f.valuation_usd) / 1000000000, 2) AS median_valuation_billion_usd
FROM read_parquet('s3://gold/fact_valuation_grit.parquet') f
JOIN read_parquet('s3://gold/dim_company.parquet') c
    ON f.company_name = c.company_name
WHERE c.country IS NOT NULL
  AND c.continent IS NOT NULL
  AND f.valuation_usd > 0
GROUP BY c.country, c.continent
HAVING COUNT(DISTINCT f.company_name) >= 1
ORDER BY avg_valuation_billion_usd DESC
LIMIT 10;
