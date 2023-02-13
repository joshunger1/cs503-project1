A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
B = LOAD '/user/ds503/Data/Transactions.txt' USING PigStorage(',') as (TransID: int, CustID: int, TransTotal: float, TransNumItems: int, TransDesc: chararray);

C = JOIN A by ID, B by CustID;
D = FOREACH C GENERATE ID, Name;

E = GROUP D BY Name;
F = FOREACH E GENERATE group, COUNT(D) AS count;

G = GROUP F ALL;
H = FOREACH G GENERATE MIN(F.count) AS minCount;

I = FILTER F BY count == H.minCount;
DUMP I;
