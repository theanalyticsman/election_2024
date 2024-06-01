{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	b.term,  
	MAX(b.year) AS YEAR, 
	AVG(a.cons_sent) AS avg_cons_sent,
	AVG(a.pct_change) AS avg_cons_sent_pct_change,
	stddev(a.cons_sent) AS std_cons_sent
FROM {{ source('source_data', 'consumer_sentiment') }} a
INNER JOIN {{ source('source_data', 'pres_terms') }} b ON a.YEAR =  b.YEAR 
GROUP BY
	b.term