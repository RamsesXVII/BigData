CREATE VIEW avg_score as
	SELECT ROW_NUMBER() OVER (PARTITION BY from_unixtime(r.time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga,
	 productId, from_unixtime(r.time,'Y-MM') as annoMese, avg(score) as media
	FROM reviews r
	GROUP BY from_unixtime(r.time,'Y-MM'), productId 
	ORDER BY annoMese, media desc;



SELECT sq.productId, sq.annoMese, sq.media
FROM  avg_score sq
WHERE sq.numeroRiga<6
ORDER BY sq.annoMese, sq.media desc;