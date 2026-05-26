SELECT UserID, EXTRACT(MINUTE FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM clickbench GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
