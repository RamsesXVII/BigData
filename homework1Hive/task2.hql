SELECT sq.userId, collect_set(concat(sq.productId,concat("-",sq.score)))
FROM ( SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY score desc) AS numeroriga, userId, productId, score
 	   FROM reviews
 	   ORDER BY userId, productId, score desc ) as sq
WHERE sq.numeroriga<11
GROUP BY sq.userId
ORDER BY sq.userId;