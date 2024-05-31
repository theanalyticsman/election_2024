SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a."NASDAQ") AS avg_nasdaq,
	AVG(a.pct_change) AS avg_nasdaq_pct_change,
	stddev(a."NASDAQ") AS std_nasdaq
FROM {{ source('econ_data', 'nasdaq') }} a
INNER JOIN {{ source('demo_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term
	