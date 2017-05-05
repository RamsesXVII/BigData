CREATE TABLE reviews (id STRING, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator STRING, helpfulnessDenominator STRING, score INT, time BIGINT, summary STRING, text STRING) row format delimited fields terminated by '\t';

LOAD DATA LOCAL INPATH '/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv' OVERWRITE INTO TABLE reviews;


#HIVE1
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.numeroRiga,sq.productId, sq.annoMese, sq.media from  (select ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga, productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews r group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc) as sq where sq.numeroRiga<6 order by sq.annoMese, sq.media desc;
%#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


#HIVE2
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.userId,collect_set(concat(sq.productId,concat("-",sq.score))) from (select ROW_NUMBER() OVER (PARTITION BY userId ORDER BY score desc) as numeroriga, userId, productId, score from reviews order by userId, productId, score desc) as sq where sq.numeroriga<11 group by sq.userId order by sq.userId;
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



#HIVE 3 (FACOLTATIVO)
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.utenteA, sq.utenteB, sq.numeroProdottiComuni,sq.elencoProdottiComuni from (select a.userId as utenteA, b.userId as utenteB, count(distinct a.productId) as numeroProdottiComuni, collect_set(a.productId) as elencoProdottiComuni from reviews a join reviews b on a.productId=b.productId where a.score >=4 and b.score>=4 and a.userId<>b.userId group by a.userId,b.userId) as sq
where sq.numeroProdottiComuni>=3 and sq.utenteA<sq.utenteB order by sq.utenteA,sq.utenteB;
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
