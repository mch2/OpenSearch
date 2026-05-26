SELECT AdvEngineID, COUNT(*) AS c FROM clickbench WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC
