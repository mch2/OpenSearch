SELECT SearchPhrase, COUNT(*) AS c FROM clickbench WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
