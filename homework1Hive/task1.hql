SELECT sq.numeroRiga,sq.productId, sq.annoMese, sq.media 
FROM  ( SELECT ROW_NUMBER() OVER (PARTITION BY from_unixtime(time,'Y-MM') ORDER BY avg(score) desc) AS numeroRiga,
				 productId, from_unixtime(time,'Y-MM') AS annoMese, avg(score) AS media
		FROM reviews r
		GROUP BY from_unixtime(time,'Y-MM'), productId
		ORDER BY annoMese, media desc ) AS sq
WHERE sq.numeroRiga<6
ORDER BY sq.annoMese, sq.media desc;