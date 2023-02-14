--load customer and transaction dataset into pig query
A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
B = LOAD '/user/ds503/Data/Transactions.txt' USING PigStorage(',') as (TransID: int, CustID: int, TransTotal: float, TransNumItems: int, TransDesc: chararray);
--join the two datasets by the customer id
C = JOIN A by ID, B by CustID;
--filter the data to only include, gender, transaction total, and age, 
--also make a new column for each record for the age bucket they fall into
D = FOREACH C GENERATE Gender, TransTotal, Age, 
    (CASE
        WHEN Age < 20 THEN '10-20'
        WHEN Age < 30 THEN '20-30'
        WHEN Age < 40 THEN '30-40'
        WHEN Age < 50 THEN '40-50'
        WHEN Age < 60 THEN '50-60'
        ELSE '60-70'
    END) AS AgeRange;
--group the data by the age range and gender
E = GROUP D BY (AgeRange, Gender);
--calculate min, max, and transtotal for each group
F = FOREACH E GENERATE group, MIN(D.TransTotal), MAX(D.TransTotal), AVG(D.TransTotal); 
--dump the data out
DUMP F;
--can also save to hdfs, but was more diffuclt to quickly check output
--STORE F INTO 'hdfs://localhost:9000/user/ds503/PigQuery3' USING PigStorage(',');

