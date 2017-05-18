SELECT sq.utenteA, sq.utenteB, sq.numeroProdottiComuni,sq.elencoProdottiComuni
FROM (	SELECT a.userId AS utenteA, b.userId AS utenteB, count(distinct a.productId) AS numeroProdottiComuni,
		 		collect_set(a.productId) AS elencoProdottiComuni
		FROM reviews a JOIN reviews b ON a.productId=b.productId
		WHERE a.score >=4 AND b.score>=4 AND a.userId<>b.userId
		GROUP BY a.userId,b.userId) AS sq
WHERE sq.numeroProdottiComuni>=3 AND sq.utenteA<sq.utenteB
ORDER BY sq.utenteA,sq.utenteB;
