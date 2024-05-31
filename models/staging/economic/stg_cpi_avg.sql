SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a.cpi) AS avg_cpi,
	AVG(a.pct_change) AS avg_cpi_pct_change,
	stddev(a.cpi) AS std_cpi
FROM {{ source('econ_data', 'cpi') }} a
INNER JOIN {{ source('demo_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term