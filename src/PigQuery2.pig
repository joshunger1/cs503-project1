--load customer data into the pig query
A = LOAD '/user/ds503/Data/Customers.txt' USING PigStorage(',') as (ID: int, Name: chararray, Age: int, Gender: chararray, CountryCode: int, Salary: float);
--filter the inputted data down to just the country code and ID
B = FOREACH A GENERATE CountryCode, ID;
--group the data by the country code
C = GROUP B BY CountryCode;
--calculate the amount of customers in each country coode
D = FOREACH C GENERATE group, COUNT(B) as numCustomers;
--filter to only the countries with more than 5000 or less than 2000
E = FILTER D BY numCustomers > 5000 or numCustomers < 2000;
--dump the resulting tuples
DUMP E;
--can also save to hdfs, but was more diffuclt to quickly check output
--STORE E INTO 'hdfs://localhost:9000/user/ds503/PigQuery2' USING PigStorage(',');