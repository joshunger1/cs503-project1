A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
B = FOREACH A GENERATE CountryCode, ID;
C = GROUP B BY CountryCode;
D = FOREACH C GENERATE group, COUNT(B) as numCustomers;
E = FILTER D BY numCustomers > 5000 or numCustomers < 2000;
DUMP E;
--STORE E INTO 'hdfs://localhost:9000/user/ds503/PigTestQuery2.1' USING PigStorage(',');