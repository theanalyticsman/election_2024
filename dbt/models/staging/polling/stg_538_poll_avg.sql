{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	a."cycle",
	upper(a.state) AS state,
	a.modeldate,
	CASE a.candidate_name 
		WHEN 'John Kerry' THEN 'DEMOCRAT'
		WHEN 'Barack Obama' THEN 'DEMOCRAT'
		WHEN 'Al Gore' THEN 'DEMOCRAT'
		WHEN 'Joseph R. Biden Jr.' THEN 'DEMOCRAT'
		WHEN 'Hillary Rodham Clinton' THEN 'DEMOCRAT'
		WHEN 'John McCain' THEN 'REPUBLICAN'
		WHEN 'George W. Bush' THEN 'REPUBLICAN'
		WHEN 'Mitt Romney' THEN 'REPUBLICAN'
		WHEN 'Donald Trump' THEN 'REPUBLICAN'
	END AS party,
	a.pct_estimate
FROM {{ source('source_data', 'fivethirtyeight_pres_poll_avg') }} a
INNER JOIN 
	(SELECT "cycle", MAX(CAST(modeldate AS date)) AS max_date
		FROM {{ source('source_data', 'fivethirtyeight_pres_poll_avg') }}
		GROUP BY "cycle") b ON a."cycle"  = b."cycle"
		AND CAST(a.modeldate AS DATE) = b.max_date