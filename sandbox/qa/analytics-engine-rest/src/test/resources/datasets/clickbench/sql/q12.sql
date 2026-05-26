SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM clickbench WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10
