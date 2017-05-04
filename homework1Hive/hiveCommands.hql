CREATE TABLE reviews (id STRING, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator STRING, helpfulnessDenominator STRING, score INT, time BIGINT, summary STRING, text STRING) row format delimited fields terminated by '\t';

LOAD DATA LOCAL INPATH '/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv' OVERWRITE INTO TABLE reviews;


per convertire i timestamp in formato data
SELECT from_unixtime(time) as new_timestamp from reviews;

per convertire i timestamp in formato data prendendo solo mese e anno
SELECT substring (from_unixtime(time),0,7) as new_timestamp from reviews;

per avere lo score medio di un prodotto
SELECT productId, AVG(score) from reviews group by productId;

select from_unixtime(time,'Y-MM') from reviews;

select r.productId as prodotto, r.score punteggio, from_unixtime(time,'Y-MM') giorno from reviews r;



#DROP TABLE reviews;

select productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;


select productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from


 (   select productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;)


 group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;



 select Row_Number()over(partition by productId order by productId asc),productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;



select Row_Number() over(partition by productId,from_unixtime(time,'Y-MM'),avg(score) as media group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc),productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;




 select Row_Number()over(partition by productId,from_unixtime(time,'Y-MM'),avg(score) group by order by productId asc),productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;




SELECT * FROM (select ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga, productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews r group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc) WHERE r.numero<5;




select ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga, productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews r group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc;



 select sq.numeroRiga,sq.productId, sq.annoMese, sq.media from  (select ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga, productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews r group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc) as sq where sq.numeroRiga<6 order by sq.annoMese, sq.media desc;

HIVE1
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.numeroRiga,sq.productId, sq.annoMese, sq.media from  (select ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga, productId, from_unixtime(time,'Y-MM') as annoMese, avg(score) as media from reviews r group by from_unixtime(time,'Y-MM'), productId order by annoMese, media desc) as sq where sq.numeroRiga<6 order by sq.annoMese, sq.media desc;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


Un  job che  sia in  grado  di  generare
,  per  ciascun  utente, i  10  prodotti  preferiti  (ovvero  quelli 
che ha recensito conil punteggio più alto), indicando 
ProductId e Score. Il risultato deve essere ordinato in base allo UserId

userId

HIVE2
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.numeroriga, sq.userId, sq.productId, sq.score from (select ROW_NUMBER() OVER (PARTITION BY userId ORDER BY score desc) as numeroriga, userId, productId, score from reviews order by userId, productId, score desc) as sq where sq.numeroriga<11 order by sq.userId;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

partitioning all'inizio???


select a.userId, b.userId, productId from reviews a join reviews b on a.productId=b.productId where score >4 and a.userId<>b.userId;



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
from (select a.userId as utenteA, b.userId untenteB, count(*) as numeroProdottiComuni, from reviews a join reviews b on a.productId=b.productId where a.score >=4 and b.score>=4 and a.userId<>b.userId group by a.userId,b.userId) as sq
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


qlcs che nn va (risolto; una persona giudica pù volte lo stesso prodotto)
select a.userId, b.userId, count(*), collect_set(a.productId), collect_set(b.productId) from reviews a join reviews b on a.productId=b.productId where a.score >=4 and b.score>=4 and a.userId<>b.userId group by a.userId,b.userId;


rimuovere duplicati
HIVE3
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
select sq.utenteA, sq.utenteB, sq.numeroProdottiComuni,sq.elencoProdottiComuni from (select a.userId as utenteA, b.userId as utenteB, count(distinct a.productId) as numeroProdottiComuni, collect_set(a.productId) as elencoProdottiComuni from reviews a join reviews b on a.productId=b.productId where a.score >=4 and b.score>=4 and a.userId<>b.userId group by a.userId,b.userId) as sq
where sq.numeroProdottiComuni>=3 and sq.utenteA>sq.utenteB order by sq.utenteA,sq.utenteB;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




