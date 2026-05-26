SELECT UserID, SearchPhrase, COUNT(*) FROM clickbench GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
