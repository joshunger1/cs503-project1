A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
B = LOAD '/user/ds503/Data/Transactions.txt' USING PigStorage(',') as (TransID: int, CustID: int, TransTotal: float, TransNumItems: int, TransDesc: chararray);
C = JOIN A by ID, B by CustID;

D = FOREACH C GENERATE Gender, TransTotal, Age, 
    (CASE
        WHEN Age < 20 THEN '10-20'
        WHEN Age < 30 THEN '20-30'
        WHEN Age < 40 THEN '30-40'
        WHEN Age < 50 THEN '40-50'
        WHEN Age < 60 THEN '50-60'
        ELSE '60-70'
    END) AS AgeRange;

E = GROUP D BY (AgeRange, Gender);
F = FOREACH E GENERATE group, MIN(D.TransTotal), MAX(D.TransTotal), AVG(D.TransTotal); 
DUMP F;

