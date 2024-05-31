with 

election_results as
(
    select * 
    from {{ ref("stg_election_results_county") }}
    where year > 2004
),

election_meta as 
(
    select * from {{ ref('stg_election_meta') }}
),

census as 
(
    select * from {{ ref('stg_census_data_ratios') }}
),

polls as 
(
    SELECT * from {{ ref('stg_538_poll_avg') }}
),

consumer_sentiment as 
(
    SELECT * from {{ ref('stg_consumer_sentiment_avg') }}
),

cpi as 
(
    SELECT * from {{ ref('stg_cpi_avg') }}
),

nasdaq as 
(
    SELECT * from {{ ref('stg_nasdaq_avg') }}
),

urr as 
(
    SELECT * from {{ ref('stg_urr_avg') }}
),

volatility as 
(
    SELECT * from {{ ref('stg_volatility_avg') }}
)




SELECT 
    a.year, 
    a.year + 4 as next_election,
    a.state, 
    a.county_name,
    a.fips_short,
    a.party,
    a.candidatevotes,
    a.vote_share,
    a.totalvotes,
    CAST(a.totalvotes as FLOAT) / CAST(c.population_total as FLOAT) as turnout,
	b.HARD_INCUMBENT,
	b.SOFT_INCUMBENT,
	b.PERSONAL_SCANDAL_INDEX,
	b.PRESIDENTIAL_SCANDAL_INDEX,
	b.UNPOPULAR_WAR,
    c.population_total,	
    c.income_median_income_12_month,	
    c.population_density,
    c.ratio_male,	
    c.ratio_female,	
    c.ratio_white,	
    c.ratio_hispanic,	
    c.ratio_population_black_all,	
    c.ratio_population_degree,
    d.pct_estimate as poll_average,
    f.avg_cons_sent,
    f.avg_cons_sent_pct_change,
    f.std_cons_sent,
    g.avg_cpi,
    g.avg_cpi_pct_change,
    g.std_cpi,
    h.avg_nasdaq,
    h.avg_nasdaq_pct_change,
    h.std_nasdaq,
    i.avg_urr,
    i.avg_urr_pct_change,
    i.std_urr,
    j.avg_volatility,
    j.avg_volatility_pct_change,
    j.std_volatility
FROM election_results a 
INNER JOIN election_meta b ON a.year = b.YEAR
    AND a.party = b.PARTY
LEFT JOIN census c ON a.year = c.year 
    AND a.fips_short = c.fips_short
LEFT JOIN polls d ON a.year = d.cycle
    AND a.state = d.state 
    AND a.party = d.party
LEFT JOIN consumer_sentiment f on a.year = f.year 
LEFT JOIN cpi g on a.year = g.year 
LEFT JOIN nasdaq h on a.year = h.year 
LEFT JOIN urr i on a.year = i.year 
    AND a.state = i.state
LEFT JOIN volatility j ON a.year = j.year  