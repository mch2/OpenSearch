SELECT SearchPhrase, COUNT(*) AS c FROM clickbench WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
