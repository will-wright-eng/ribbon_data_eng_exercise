# ribbon_data_eng_exercise
Data engineering take home exercise


## Questions
1) What’s the average age of the providers in ​providers_json
2) What is the most popular specialty of the providers in ​providers_json​?
3) You’ll notice within all locations objects of the ​providers_json​ record field, there exists a
“confidence” score. This is a representation of our model deciding the probability that a provider is indeed practicing at this location. We allow scores between 0 and 5. How many providers have ​at least​ 2 locations with confidence of 4 or higher?
4) In all provider records, you’ll see a field called “insurances”. This is the unique list of all insurance plans a provider accepts (we represent this as UUID which connects to the insurances table). For now, let’s assume a provider accepts all of these insurance plans at all locations they practice at. Find the total number of ​unique​ insurance plans accepted by all providers at the most ​popular​ location of this data set. (Popular = the most providers practice there)
5) Which provider fields in ​provider_json​ are the most neglected? How would you go about figuring this out beyond this small sample?
6) Who are the top 3 sources/brokers from ​provider_puf_data​ which had the most rows added or dropped between January and June.
NOTE: If a source didn’t exist in January or June, treat that source like it has 0 rows for that date.
7) Using only the “address” string in the address column of ​provider_puf_data​, which NPIs had the most new addresses added between January and June and how many new addresses were added? (the top 3 NPIs will do)
8) Now the opposite of #7, which NPIs saw the most addresses removed between January and June? (the top 3 NPIs will do)
9) How did PUF plans within the plans field of ​provider_puf_data​ change from January to June? This is intentionally a bit open ended :)
10) If you look closely at the address strings during exercises 7/8, you’ll notice a lot of redundant addresses that are just slightly different. If we could merge these addresses we could get much better resolution on which addresses are actually changing between January and June. Given the data you have here, how would you accomplish this? What if you could use any tools available? How would you begin architecting this for now and the future.
11) How long did it take to complete the exercise? (To be fair to candidates who are time constrained we like to be aware of this)


## Answers
### 1) What’s the average age of the providers in ​providers_json

NOTE TO SELF: 18 YEARS OLD SEEMS VERY YOUNG, LOOK AT HISTOGRAM DISTRIBUTION --> TABLE OF AGE AND AGE COUNTS

```sql
SELECT 
	SUM (CAST (record ->> 'age' AS INTEGER)) as total_age, 
	COUNT(*) as rows, 
	SUM (CAST (record ->> 'age' AS INTEGER))/COUNT(*) as avg
from provider_json
```
time: 488 ms

_Results_
- The average age of all providers is 18 years old

|total_age|rows|avg|
|:-:|:-:|:-:|
| 177045 | 9655 | 18 | 

### 2) What is the most popular specialty of the providers in ​providers_json​?

NOTE TO SELF: REDUCE CTEs AND CHECK LENGTH BEFORE AND AFTER JOIN IN CASE OF FANNING

```sql 
with a as (
	select 
		json_array_elements(record -> 'specialties') #>> '{}' as id 
	from provider_json
	),

b as (
	select s.display 
	from a
	left join specialties as s
		on cast(a.id as uuid) = s.uuid
	)

select 
	*, 
	count(*) 
from b
group by 1
order by 2 desc
```
time: 564 ms

_Results_
- The most popular was __Internal Medicine__
- The top three are displayed

| display|count|
|:-:|:-:|
| Internal Medicine | 1758 |
| Family Medicine | 961 |
| Pharmacist | 778 |

### 3) You’ll notice within all locations objects of the ​providers_json​ record field, there exists a “confidence” score. This is a representation of our model deciding the probability that a provider is indeed practicing at this location. We allow scores between 0 and 5. How many providers have ​at least​ 2 locations with confidence of 4 or higher?

NOTE TO SELF: CHECK TO MAKE SURE THERE ARE 4284 ROWS WHEN LISTING OUT ALL PROVIDERS AND PROVIDER LOCATIONS (IE LEN(B))

```sql
with a as (
	select 
		record ->> 'npi' as npi,
		cast((json_array_elements(record -> 'locations') ->> 'confidence') as int) as confidence,
		1 as counter
	from provider_json
),

b as (
	select 
		*
	from a
	where confidence >= 4
),

c as (
	select 
		npi, 
		sum(counter) as counter 
	from b
	group by 1
)

select 
	count(*) 
from c
where counter>=2
```
time: 2.9 s

_Results_
- There are __2054 providers__ with greater than or equal to 2 locations with a confidence score of greater than or equal to 4

### 4) In all provider records, you’ll see a field called “insurances”. This is the unique list of all insurance plans a provider accepts (we represent this as UUID which connects to the insurances table). For now, let’s assume a provider accepts all of these insurance plans at all locations they practice at. Find the total number of ​unique​ insurance plans accepted by all providers at the most ​popular​ location of this data set. (Popular = the most providers practice there)

NOTE TO SELF: CHECK FOR PROVIDER TYPE = DOCTOR

1. create list of locations with npi attached to each
2. aggregate on location and determine most popular location by count
3. exclude rows in dataset that don't include most popular location
4. create distinct list of insurance plans used by providers that practice at this location


This sequence give you the number of occurances of each insurance plan
```sql
with a as (
	select
		record ->> 'npi' as npi,
		json_array_elements(record -> 'insurances') #>> '{}' as id
	from provider_json as p
--	limit 300
),

b as (
	select *
	from a
	left join insurances as i
	on i.uuid = cast(a.id as uuid)
)

select display, count(*) from b
group by 1
order by 2 desc
```

### 5) Which provider fields in ​provider_json​ are the most neglected? How would you go about figuring this out beyond this small sample?

```sql

```
time:

_Results_
- 

### 6) Who are the top 3 sources/brokers from ​provider_puf_data​ which had the most rows added or dropped between January and June.
NOTE: If a source didn’t exist in January or June, treat that source like it has 0 rows for that date.

```sql

```
time:

_Results_
- 

### 7) Using only the “address” string in the address column of ​provider_puf_data​, which NPIs had the most new addresses added between January and June and how many new addresses were added? (the top 3 NPIs will do)

```sql

```
time:

_Results_
- 

### 8) Now the opposite of #7, which NPIs saw the most addresses removed between January and June? (the top 3 NPIs will do)

```sql

```
time:

_Results_
- 

### 9) How did PUF plans within the plans field of ​provider_puf_data​ change from January to June? This is intentionally a bit open ended :)

```sql

```
time:

_Results_
- 

### 10) If you look closely at the address strings during exercises 7/8, you’ll notice a lot of redundant addresses that are just slightly different. If we could merge these addresses we could get much better resolution on which addresses are actually changing between January and June. Given the data you have here, how would you accomplish this? What if you could use any tools available? How would you begin architecting this for now and the future.

```sql

```
time:

_Results_
- 

### 11) How long did it take to complete the exercise? (To be fair to candidates who are time constrained we like to be aware of this)



## Schema Descriptions
### provider_json​ (npi bigint, record json):
This table represents the most accurate data we have for a given provider. The NPI is the national provider identifier. The NPI is how we identify providers across the country. The record is a JSON blob representing all data we have for this provider. In order to access this data you will need to be able to query JSON data. Above is a helpful link for doing so.

### insurances​ (uuid uuid, display text):
This table contains the unique insurance plans in our DB.

### specialties​ (uuid uuid, display text):
This table contains the unique specialties in our DB

### provider_puf_data​ (npi bigint, source text, address json, plans json, created_at timestamp): 
This table contains a couple snapshots of government data we ingest. For this table, we ingested data in January and June of this year, you can leverage the “created_at” field to differentiate these times. The source field here represents a unique insurance broker. The address and plans fields represent the address and plans the broker has for this NPI. You can assume there is a uniqueness constraint on (npi, source, address, created_at). Thus, one source/broker could have many addresses and plan combinations for each NPI.
