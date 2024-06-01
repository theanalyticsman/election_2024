{{ config(
    schema=var('stage_schema')
) }}

SELECT 
	YEAR, 
	state, 
	state_po,
	county_name,
	CASE length(county_fips)
		WHEN 5 THEN county_fips
		WHEN 4 THEN '0' || county_fips
		WHEN 2 THEN '000' || county_fips
	END AS fips_short,
	CASE WHEN party NOT IN ('DEMOCRAT','REPUBLICAN') THEN 'OTHER' ELSE party END AS party,
	SUM(candidatevotes) AS candidatevotes,
	AVG(totalvotes) AS totalvotes,
    SUM(CAST(candidatevotes as FLOAT)) / AVG(CAST(totalvotes as FLOAT)) as vote_share
FROM {{ source('source_data', 'countypres') }}
WHERE length(county_fips) < 7	
    AND totalvotes > 0
GROUP BY
	YEAR, 
	state, 
	state_po,
	county_name,
	county_fips,
	party