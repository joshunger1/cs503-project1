--load the customer and transaction datasets into pig query
A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
B = LOAD '/user/ds503/Data/Transactions.txt' USING PigStorage(',') as (TransID: int, CustID: int, TransTotal: float, TransNumItems: int, TransDesc: chararray);

--join the two datasets by the customer id
C = JOIN A by ID, B by CustID;
--filter the data down to just customer name and ID
D = FOREACH C GENERATE ID, Name;
--group the datasets by the customer name
E = GROUP D BY Name;
--count the amount of transactions each customer has completed
F = FOREACH E GENERATE group, COUNT(D) AS count;
--group the data (wouldnt let me do another aggregation without grouping again?)
G = GROUP F ALL;
--calculate the minimum count of all customer transactions
H = FOREACH G GENERATE MIN(F.count) AS minCount;
--filter the dataset to only include the records with the minimum amount of transactions
I = FILTER F BY count == H.minCount;
--dump the resulting data
DUMP I;

--can also store the data, but it was easier to test the query
--by just dumping it instead of doing hdfs gets each run
--STORE I INTO 'hdfs://localhost:9000/user/ds503/PigQuery1' USING PigStorage(',');