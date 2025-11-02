SELECT 
    SUBSTRING(IDPeriode, 1, 4) AS Year,
    SUM(MontantDeclaration) AS TotalDeclaration
FROM FactDeclaration
GROUP BY SUBSTRING(IDPeriode, 1, 4)
ORDER BY Year
