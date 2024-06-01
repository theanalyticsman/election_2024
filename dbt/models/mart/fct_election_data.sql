{{ config(
    schema=var('model_schema')
) }}


with 

election_data as 
(
    select * from {{ ref("dim_election_data") }}
)

-- SELECT * FROM election_data

select 
    b.year, 
    a.state,
    a.county_name,
    a.fips_short, 
    a.party, 

    b.candidatevotes as candidatevotes, 
    a.candidatevotes as candidatevotes_prior_election,
    b.candidatevotes - a.candidatevotes as candidatevotes_prior_election_delta,

    b.vote_share, 
    a.vote_share as vote_share_prior_election,
    b.vote_share - a.vote_share as vote_share_prior_election_delta,

    b.totalvotes,
    a.totalvotes as totalvotes_prior_election,
    b.totalvotes - a.totalvotes as totalvotes_prior_election_delta, 

    b.turnout, 
    a.turnout as turn_prior_election, 
    b.turnout - a.turnout as turn_prior_election_delta,

    b.campaign_individual,
    a.campaign_individual as campaign_individual_prior_election,
    b.campaign_individual - a.campaign_individual as campaign_individual_prior_election_delta,

    b.campaign_total,
    a.campaign_total as campaign_total_prior_election,
    b.campaign_total - a.campaign_total as campaign_total_prior_election_delta,

    b.population_total,
    a.population_total as population_total_prior_election,
    a.population_total - b.population_total as population_total_prior_election_delta,

    b.population_density,
    a.population_density as population_density_prior_election,
    b.population_density - a.population_density as population_density_prior_election_delta, 

    b.ratio_male,
    a.ratio_male as ratio_male_prior_election,
    b.ratio_male - a.ratio_male as ratio_male_prior_election_delta, 

    b.ratio_female,
    a.ratio_female as ratio_female_prior_election, 
    b.ratio_female - a.ratio_female as ratio_female_prior_election_delta,

    b.ratio_hispanic,
    a.ratio_hispanic as ratio_hispanic_prior_election, 
    b.ratio_hispanic - a.ratio_hispanic as ratio_hispanic_prior_election_delta,

    b.ratio_population_black_all,
    a.ratio_population_black_all as ratio_population_black_all_prior_election,
    b.ratio_population_black_all - a.ratio_population_black_all as ratio_population_black_all_prior_election_delta, 

    b.ratio_population_degree,
    a.ratio_population_degree as ratio_population_degree_prior_election,
    b.ratio_population_degree - a.ratio_population_degree as ratio_population_degree_prior_election_delta,

    b.poll_average,
    b.avg_cons_sent,
    b.avg_cons_sent_pct_change,
    b.avg_cpi,
    b.avg_cpi_pct_change,
    b.avg_nasdaq,
    b.avg_nasdaq_pct_change,
    b.avg_urr,
    b.avg_urr_pct_change,
    b.avg_volatility,
    b.avg_volatility_pct_change
    
from election_data a 
INNER JOIN election_data b ON a.next_election = b.year
    AND a.fips_short = b.fips_short
    AND a.party = b.party