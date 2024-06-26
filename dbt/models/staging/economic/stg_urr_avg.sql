{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	b.term, 
	UPPER(a.state) as state, 
	MAX(b.year) AS YEAR, 
	AVG(a.urr) AS avg_urr, 
	AVG(a.urr_pct_change) AS avg_urr_pct_change,
    stddev(a.urr) AS std_urr
FROM {{ source('source_data', 'unemployment') }} a
INNER JOIN {{ source('source_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term, 
	a.state