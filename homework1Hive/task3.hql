CREATE VIEW similaruser as
SELECT a.userId as utenteA, b.userId as utenteB, count(distinct a.productId) as numeroProdottiComuni,
 collect_set(a.productId) as elencoProdottiComuni
FROM reviews a
JOIN reviews b on a.productId=b.productId
WHERE a.score >=4 and b.score>=4 and a.userId<b.userId
GROUP BY a.userId,b.userId;

SELECT sq.utenteA, sq.utenteB, sq.numeroProdottiComuni, sq.elencoProdottiComuni
FROM similaruser sq
WHERE sq.numeroProdottiComuni>=3
ORDER BY sq.utenteA,sq.utenteB;