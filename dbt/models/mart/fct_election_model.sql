{{ config(
    schema=var('model_schema')
) }}


with 

election_data as 
(
    select * from {{ ref("dim_election_data") }}
)

SELECT 
	"year",
	state,
	county_name,
	fips_short,
	party,
	candidatevotes,
	candidatevotes_prior_election,
	totalvotes,
	totalvotes_prior_election,
	campaign_individual_prior_election_delta,
	population_total_prior_election_delta,
	population_density_prior_election_delta,
	ratio_male_prior_election_delta,
	ratio_female_prior_election_delta,
	ratio_hispanic_prior_election_delta,
	ratio_population_black_all_prior_election_delta,
	ratio_population_degree_prior_election_delta,
	poll_average,
	avg_cons_sent_pct_change,
	avg_cpi_pct_change,
	avg_nasdaq_pct_change,
	avg_urr_pct_change,
	avg_volatility_pct_change
FROM pres_election_mdl.fct_election_data
WHERE YEAR = '2012'
