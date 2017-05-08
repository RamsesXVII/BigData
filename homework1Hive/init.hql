CREATE TABLE reviews (id STRING, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator STRING, helpfulnessDenominator STRING, score INT, time BIGINT, summary STRING, text STRING) row format delimited fields terminated by '\t';

LOAD DATA LOCAL INPATH '/home/mattia/Scaricati/amazon/dati.csv' OVERWRITE INTO TABLE reviews;
