{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a.volatility) AS avg_volatility,
	AVG(a.pct_change) AS avg_volatility_pct_change,
	stddev(a.volatility) AS std_volatility
FROM {{ source('source_data', 'volatility') }} a
INNER JOIN {{ source('source_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term
	