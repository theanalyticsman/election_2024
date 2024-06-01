{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a."NASDAQ") AS avg_nasdaq,
	AVG(a.pct_change) AS avg_nasdaq_pct_change,
	stddev(a."NASDAQ") AS std_nasdaq
FROM {{ source('source_data', 'nasdaq') }} a
INNER JOIN {{ source('source_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term
	