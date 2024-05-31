SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a.volatility) AS avg_volatility,
	AVG(a.pct_change) AS avg_volatility_pct_change,
	stddev(a.volatility) AS std_volatility
FROM {{ source('econ_data', 'volatility') }} a
INNER JOIN {{ source('demo_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term
	