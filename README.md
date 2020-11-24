# Ribbon Health -- Data Engineering Exercise
Data engineering take home exercise

## Table of Contents
- [Answers](README.md#answers)
- [Questions](README.md#questions)
- [Schema Descriptions](README.md#schema-descriptions)

## Answers
### 1) What’s the average age of the providers in ​providers_json

```sql
SELECT 
	SUM (CAST (record ->> 'age' AS INTEGER)) AS total_age, 
	COUNT(*) AS rows, 
	SUM (CAST (record ->> 'age' AS INTEGER))/COUNT(*) AS avg
FROM provider_json
WHERE CAST (record ->> 'age' AS INTEGER) IS NOT NULL
```
time: 488 ms

_Results_
- The average age of all providers is __57 years old__, excluding null values.

|total_age|rows|avg|
|:-:|:-:|:-:|
| 177045 | 3058 | 57 | 

### 2) What is the most popular specialty of the providers in ​providers_json​?

```sql 
WITH specs AS (
	SELECT 
		json_array_elements(record -> 'specialties') #>> '{}' AS id 
	FROM provider_json
	),

spec_name AS (
	SELECT 
		s.display 
	FROM specs
	LEFT JOIN specialties AS s
		ON CAST(specs.id AS uuid) = s.uuid
	)

SELECT 
	display, 
	COUNT(*) 
FROM spec_name
GROUP BY 1
ORDER BY 2 DESC
```
time: 564 ms

_Results_
- The most popular specialty is __Internal Medicine__.
- The top three are displayed below.
- CTEs `specs` and `spec_name` have the same row count so there is no fanning -- a check for duplication in the `specialties` table.

| display|count|
|:-:|:-:|
| Internal Medicine | 1758 |
| Family Medicine | 961 |
| Pharmacist | 778 |

### 3) You’ll notice within all locations objects of the ​providers_json​ record field, there exists a “confidence” score. This is a representation of our model deciding the probability that a provider is indeed practicing at this location. We allow scores between 0 and 5. How many providers have ​at least​ 2 locations with confidence of 4 or higher?

```sql
WITH conf_score AS (
	SELECT 
		record ->> 'npi' AS npi,
		CAST((json_array_elements(record -> 'locations') ->> 'confidence') AS int) AS confidence
	FROM provider_json
),
conf_score_subset AS (
	SELECT 
		npi,
		COUNT(*) AS counter
	FROM conf_score
	WHERE confidence >= 4
	GROUP BY 1
)

SELECT 
	COUNT(*) 
FROM conf_score_subset
WHERE counter>=2
```
time: 2.9 s

_Results_
- There are __2054 providers__ with greater than or equal to 2 locations with a confidence score of greater than or equal to 4

### 4) In all provider records, you’ll see a field called “insurances”. This is the unique list of all insurance plans a provider accepts (we represent this AS UUID which connects to the insurances table). For now, let’s assume a provider accepts all of these insurance plans at all locations they practice at. Find the total number of ​unique​ insurance plans accepted by all providers at the most ​popular​ location of this data set. (Popular = the most providers practice there)

```sql
WITH pop_loc AS (
	SELECT 
		json_array_elements(record -> 'locations') ->> 'uuid' AS loc, 
		count(*) 
	FROM provider_json 
	GROUP BY 1 
	ORDER BY 2 DESC
	LIMIT 1
),
loc_list AS (
	SELECT 
		npi,
		record,
		json_array_elements(record -> 'locations') ->> 'uuid' AS loc 
	FROM provider_json
),
insur_list AS (
	SELECT 
		npi,
		loc,
		json_array_elements(record -> 'insurances') #>> '{}' AS insur_id 
	FROM loc_list
	WHERE loc = (
		SELECT 
			loc 
		FROM pop_loc)
)

SELECT DISTINCT 
	insur_id 
FROM insur_list
```
time: 1.6 s

_Results_
- There are __361 unique insurance plans__ accepted accross providers at the most popular location of this data set (uuid='50c425a1-4cdd-49fe-9ce6-25fc85938262').

### 5) Which provider fields in ​provider_json​ are the most neglected? How would you go about figuring this out beyond this small sample?

```sql
SELECT 
	CAST(SUM( CASE WHEN (record ->> 'age'			) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS age,
	CAST(SUM( CASE WHEN (record ->> 'degrees'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS degrees,
	CAST(SUM( CASE WHEN (record ->> 'educations'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS educations,
	CAST(SUM( CASE WHEN (record ->> 'first_name'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS first_name,
	CAST(SUM( CASE WHEN (record ->> 'gender'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS gender,
	CAST(SUM( CASE WHEN (record ->> 'insurances'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS insurances,
	CAST(SUM( CASE WHEN (record ->> 'languages'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS languages,
	CAST(SUM( CASE WHEN (record ->> 'last_name'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS last_name,
	CAST(SUM( CASE WHEN (record ->> 'locations'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS locations,
	CAST(SUM( CASE WHEN (record ->> 'middle_name'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS middle_name,
	CAST(SUM( CASE WHEN (record ->> 'online_profiles'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS online_profiles,
	CAST(SUM( CASE WHEN (record ->> 'provider_types'	) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS provider_types,
	CAST(SUM( CASE WHEN (record ->> 'ratings_avg'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS ratings_avg,
	CAST(SUM( CASE WHEN (record ->> 'ratings_count'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS ratings_count,
	CAST(SUM( CASE WHEN (record ->> 'specialties'		) is NULL THEN 0 ELSE 1 END ) AS FLOAT)/ COUNT(*) AS specialties 
FROM provider_json
```
time: 6.3 s

_Results_
- Without having much context for the contents I would first check for completness of the table. If certain fields are majority null, such AS `age`, I would concider them neglected.

| field|fraction_not_null|
|:-:|:-:|
| age | 0.32 |
| ratings_avg | 0.68 |
| gender | 0.76 |
| middle_name | 0.79 |

- As mentioned above, looking at the completness of a field is a good first check. I would also check consistency -- where the metric(s) are consistently represented accross sources and over time. For example, are strings in the education field diverse in their spelling of the same institution or do they conform to some standard (in this case they do appear to conform to a standard and therefore should not be considered "neglected").

### 6) Who are the top 3 sources/brokers from ​provider_puf_data​ which had the most rows added or dropped between January and June.
NOTE: If a source didn’t exist in January or June, treat that source like it has 0 rows for that date.

```sql
WITH june AS (
	SELECT 
		source, 
		COUNT(*) AS counts 
	FROM provider_puf_data 
	WHERE  DATE(created_at) = '2019-06-25' 
	GROUP BY 1 
	ORDER BY 2 DESC
),
jan AS (
	SELECT 
		source, 
		COUNT(*) AS counts 
	FROM provider_puf_data 
	WHERE  DATE(created_at) = '2019-01-08' 
	GROUP BY 1 
	ORDER BY 2 DESC)

SELECT 
	june.source, 
	jan.counts AS count_jan, 
	june.counts AS count_june, 
	(june.counts - jan.counts) AS count_diff,
	ABS(june.counts - jan.counts) AS abs_count_diff
FROM june
LEFT JOIN jan
	ON june.source = jan.source
ORDER BY 4 DESC
```
time: 696 ms

_Results_
- __dentegra.com__ has the most absolute difference between the January data dump and the June data dump. 

| source|count_diff|abs_count_diff|
|:-:|:-:|:-:|
| dentegra.com | 604 | 604 |
| api.centene.com | -237 | 237 |
| d3ul0st9g52g6o.cloudfront.net | 220 | 220 |

### 7) Using only the “address” string in the address column of ​provider_puf_data​, which NPIs had the most new addresses added between January and June and how many new addresses were added? (the top 3 NPIs will do)

```sql
WITH june AS (
	SELECT distinct
		npi, 
		address::json->>'address' AS _address 
	FROM ​provider_puf_data​
	WHERE DATE(created_at) = '2019-06-25'
	GROUP BY 1,2
),
jan AS (
	SELECT distinct
		npi, 
		address::json->>'address' AS _address 
	FROM provider_puf_data 
	WHERE DATE(created_at) = '2019-01-08' 
	GROUP BY 1,2
),
combined AS (
	SELECT 
		COALESCE(june.npi,jan.npi) AS npi,
		june._address AS june_address, 
		jan._address AS jan_address
	FROM june
	FULL OUTER JOIN jan 
		ON june._address = jan._address
	WHERE june._address is NULL
		AND jan._address IS NOT NULL
)

SELECT 
	npi, 
	COUNT(*) 
FROM combined
GROUP BY 1 
ORDER BY 2 DESC
```
time: 503 ms

_Results_
- I'm assuming there is only one json 'address' within each row element of the address field. FROM the rows I've looked at this appears to be the case.
- Duplication is an issue so I'm assuming that new addresses implies _unique_ addresses.
- I'm also excluding the case WHERE the same address is available FROM multiple NPIs (ie coalesce will populate the first non-null NPI between the two data deliveries)
 - In the case WHERE there are known address-duplicates provided by multiple NPIs, I would JOIN on NPI AS well AS address and check for fanning.

| npi|count|
|:-:|:-:|
| 1659334894 | 74 |
| 1073694808 | 31 |
| 1669471462 | 29 |

### 8) Now the opposite of #7, which NPIs saw the most addresses removed between January and June? (the top 3 NPIs will do)

```sql
WITH june AS (
	SELECT distinct
		npi, 
		address::json->>'address' AS _address 
	FROM ​provider_puf_data​
	WHERE DATE(created_at) = '2019-06-25'
	GROUP BY 1,2
),
jan AS (
	SELECT distinct
		npi, 
		address::json->>'address' AS _address 
	FROM provider_puf_data 
	WHERE DATE(created_at) = '2019-01-08' 
	GROUP BY 1,2
),
combined AS (
	SELECT 
		COALESCE(june.npi,jan.npi) AS npi,
		june._address AS june_address, 
		jan._address AS jan_address
	FROM june
	FULL OUTER JOIN jan 
		ON june._address = jan._address
	WHERE june._address IS NOT NULL
		AND jan._address is NULL
)

SELECT 
	npi, 
	COUNT(*) 
FROM combined
GROUP BY 1 
ORDER BY 2 DESC
```
time: 443 ms

_Results_
- Same assumptions as above.
- The only change in the query is the final where-statement specifying which address column should be null and which should _not_ be null, as a result of the join.

| npi|count|
|:-:|:-:|
| 1114191335 | 66 |
| 1265411953 | 63 |
| 1801806518 | 63 |

### 9) How did PUF plans within the plans field of ​provider_puf_data​ change from January to June? This is intentionally a bit open ended :)

For example:
```sql
WITH jan_eg AS (
	SELECT 
		json_array_elements(plans) AS _plans
	FROM ​provider_puf_data​
	WHERE  DATE(created_at) = '2019-01-08'
),
jan_eg_plans AS (
	SELECT distinct
		_plans ->> 'plan_id' AS plan_id
	FROM jan_eg
	GROUP BY 1
)

SELECT 
	COUNT(*) 
FROM jan_eg_plans
```
time 11.8 s

_Results_
- There are fewer total rows in June compared to Janary, which correspond to less distinct plan_ids and distinct network_tiers.
- The `~600` less rows do not seem to correspond to the reduction in plan_ids and total_plans, therefore I would say there was a contraction in the total offerings by insurers over the course of the 5 months (this assumes the data provided is representative of the actual insurance space and not a fault of the data provider).
- Note: the query above is an example of the pattern I used to populate the table below, switching out date and json keys for element.

| created_at|total_rows|total_plans| distinct_plan_id | distinct_network_tier | distinct_plan_id_type |
|:-:|:-:|:-:|:-:|:-:|:-:|
| '2019-01-08' | 42,319 | 4,818,337 | 6174 | 101 | 1 |
| '2019-06-25' | 41,681 | 4,621,730 | 5420 | 84 | 1 |
| difference 	| 638 | 496,607 	| 754 | 17 | 0 |

### 10) If you look closely at the address strings during exercises 7/8, you’ll notice a lot of redundant addresses that are just slightly different. If we could merge these addresses we could get much better resolution on which addresses are actually changing between January and June. Given the data you have here, how would you accomplish this? What if you could use any tools available? How would you begin architecting this for now and the future.

This is an interesting problem because I believe it can be approached from a few different angles. My approach would be to (1) reduce the size of the problem (2) generate a sort of internal validation, referenceing the dataset itself, (3) validate to external source(s) and then (4) merge based of the metrics produced.

1. I would first break the problem of address merging up into many smaller segments, since it doesn't seem efficient to try and match fuzzy strings accross the entire dataset -- nor realistic that there aren't duplicate addresses accross the US that are each real. I would do this by partitioning the datset into the smallest trusted piece of information. Ideally this would be by some small geohashed block or polygon but realistically it will be by a state, city, or zip code. 
2. By scoring each address according to it's prevelance within the segment and by it's Levenshtein distance (or a simmilar fuzzy match algorithm) to the other addresses you can start to get a sense for which addresses are real and potentially related respectively. 
3. Additional information from external sources such AS the Google Maps API would help determine if there were in fact duplicate addresses of the same name within your segment area. If not cost prohibitive, outsourcing the confirmation process to a SaaS company like [Premise](https://www.premise.com/) could be done.
4. Merge addresses based on the following cirteria:
 - low prevelance,
 - high fuzzy match with other addresses in segment, and
 - no valid duplicates
 The high and low values would be determined by quartiles, or in cases where there is a high error rate then histogram bins can help deliniate -- manually checking a sample of the addresses to be merged for accuracy. 

### 11) How long did it take to complete the exercise? (To be fair to candidates who are time constrained we like to be aware of this)

It took me about 7 hours total, spread out over the course of 4 days. 

----
----

## Questions
1) What’s the average age of the providers in ​providers_json
2) What is the most popular specialty of the providers in ​providers_json​?
3) You’ll notice within all locations objects of the ​providers_json​ record field, there exists a
“confidence” score. This is a representation of our model deciding the probability that a provider is indeed practicing at this location. We allow scores between 0 and 5. How many providers have ​at least​ 2 locations with confidence of 4 or higher?
4) In all provider records, you’ll see a field called “insurances”. This is the unique list of all insurance plans a provider accepts (we represent this AS UUID which connects to the insurances table). For now, let’s assume a provider accepts all of these insurance plans at all locations they practice at. Find the total number of ​unique​ insurance plans accepted by all providers at the most ​popular​ location of this data set. (Popular = the most providers practice there)
5) Which provider fields in ​provider_json​ are the most neglected? How would you go about figuring this out beyond this small sample?
6) Who are the top 3 sources/brokers from ​provider_puf_data​ which had the most rows added or dropped between January and June.
NOTE: If a source didn’t exist in January or June, treat that source like it has 0 rows for that date.
7) Using only the “address” string in the address column of ​provider_puf_data​, which NPIs had the most new addresses added between January and June and how many new addresses were added? (the top 3 NPIs will do)
8) Now the opposite of #7, which NPIs saw the most addresses removed between January and June? (the top 3 NPIs will do)
9) How did PUF plans within the plans field of ​provider_puf_data​ change from January to June? This is intentionally a bit open ended :)
10) If you look closely at the address strings during exercises 7/8, you’ll notice a lot of redundant addresses that are just slightly different. If we could merge these addresses we could get much better resolution on which addresses are actually changing between January and June. Given the data you have here, how would you accomplish this? What if you could use any tools available? How would you begin architecting this for now and the future.
11) How long did it take to complete the exercise? (To be fair to candidates who are time constrained we like to be aware of this)

## Schema Descriptions
### provider_json​ (npi bigint, record json):
This table represents the most accurate data we have for a given provider. The NPI is the national provider identifier. The NPI is how we identify providers across the country. The record is a JSON blob representing all data we have for this provider. In order to access this data you will need to be able to query JSON data. Above is a helpful link for doing so.

### insurances​ (uuid uuid, display text):
This table contains the unique insurance plans in our DB.

### specialties​ (uuid uuid, display text):
This table contains the unique specialties in our DB

### provider_puf_data​ (npi bigint, source text, address json, plans json, created_at timestamp): 
This table contains a couple snapshots of government data we ingest. For this table, we ingested data in January and June of this year, you can leverage the “created_at” field to differentiate these times. The source field here represents a unique insurance broker. The address and plans fields represent the address and plans the broker has for this NPI. You can assume there is a uniqueness constraint on (npi, source, address, created_at). Thus, one source/broker could have many addresses and plan combinations for each NPI.
