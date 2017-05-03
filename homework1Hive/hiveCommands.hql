CREATE TABLE amazonFinefoodReviews (id STRING, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator STRING, helpfulnessDenominator STRING, score INT, time BIGINT, sumamry STRING, text STRING) row format delimited fields terminated by '\t';

LOAD DATA LOCAL INPATH '/home/iori/Downloads/FineFoodReviews/amazon/prime1099-06.csv' OVERWRITE INTO TABLE amazonFinefoodReviews;
