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

```sql
SELECT 
	SUM (CAST (record ->> 'age' AS INTEGER)) as total_age, 
	COUNT(*) as rows, 
	SUM (CAST (record ->> 'age' AS INTEGER))/COUNT(*) as avg
from provider_json
```

| total_age    | rows		  | avg |
|      :-:     |      :-:     | :-: |
| 177045 | 9655 | 18 | 

### 2) What is the most popular specialty of the providers in ​providers_json​?

### 3) You’ll notice within all locations objects of the ​providers_json​ record field, there exists a
“confidence” score. This is a representation of our model deciding the probability that a provider is indeed practicing at this location. We allow scores between 0 and 5. How many providers have ​at least​ 2 locations with confidence of 4 or higher?

### 4) In all provider records, you’ll see a field called “insurances”. This is the unique list of all insurance plans a provider accepts (we represent this as UUID which connects to the insurances table). For now, let’s assume a provider accepts all of these insurance plans at all locations they practice at. Find the total number of ​unique​ insurance plans accepted by all providers at the most ​popular​ location of this data set. (Popular = the most providers practice there)

### 5) Which provider fields in ​provider_json​ are the most neglected? How would you go about figuring this out beyond this small sample?

### 6) Who are the top 3 sources/brokers from ​provider_puf_data​ which had the most rows added or dropped between January and June.
NOTE: If a source didn’t exist in January or June, treat that source like it has 0 rows for that date.

### 7) Using only the “address” string in the address column of ​provider_puf_data​, which NPIs had the most new addresses added between January and June and how many new addresses were added? (the top 3 NPIs will do)

### 8) Now the opposite of #7, which NPIs saw the most addresses removed between January and June? (the top 3 NPIs will do)

### 9) How did PUF plans within the plans field of ​provider_puf_data​ change from January to June? This is intentionally a bit open ended :)

### 10) If you look closely at the address strings during exercises 7/8, you’ll notice a lot of redundant addresses that are just slightly different. If we could merge these addresses we could get much better resolution on which addresses are actually changing between January and June. Given the data you have here, how would you accomplish this? What if you could use any tools available? How would you begin architecting this for now and the future.

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
