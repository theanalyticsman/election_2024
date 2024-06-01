SELECT 
	"YEAR" as year,
	"PARTY" as party,
	"HARD_INCUMBENT" as HARD_INCUMBENT,
	"SOFT_INCUMBENT" as SOFT_INCUMBENT,
	"PERSONAL_SCANDAL_INDEX" as PERSONAL_SCANDAL_INDEX,
	"PRESIDENTIAL_SCANDAL_INDEX" as PRESIDENTIAL_SCANDAL_INDEX,
	"UNPOPULAR_WAR" as UNPOPULAR_WAR,
    campaign_individual, 
    campaign_total
FROM {{ source('demo_data', 'election_party_meta') }}