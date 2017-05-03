CREATE TABLE reviews (id STRING, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator STRING, helpfulnessDenominator STRING, score INT, time BIGINT, summary STRING, text STRING) row format delimited fields terminated by '\t';

LOAD DATA LOCAL INPATH '/home/iori/Downloads/FineFoodReviews/amazon/prime1099-06.csv' OVERWRITE INTO TABLE reviews;


per convertire i timestamp in formato data
SELECT from_unixtime(time) as new_timestamp from reviews;

per convertire i timestamp in formato data prendendo solo mese e anno
SELECT substring (from_unixtime(time),0,7) as new_timestamp from reviews;


#DROP TABLE reviews;