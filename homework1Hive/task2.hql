CREATE VIEW userproducts as
	SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY score desc) as numeroriga, userId, productId, score
	FROM reviews
	ORDER BY userId, productId, score desc;

SELECT sq.userId, collect_set(concat(sq.productId,concat("-",sq.score)))
FROM userproducts sq
WHERE sq.numeroriga<11
GROUP BY sq.userId
ORDER BY sq.userId;