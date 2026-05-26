SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM clickbench WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10
