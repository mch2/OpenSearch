SELECT UserID, SearchPhrase, COUNT(*) FROM clickbench GROUP BY UserID, SearchPhrase LIMIT 10
