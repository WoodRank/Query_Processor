# Query_Processor
Creating a Volcano Iterator model in C++.

How to run the program:
The executeable file is in the "build" directory and this is a sample way to execute:

./query_processor ../plans/query_recent_usa_discount.json ../data/

The "../plans/query_recent_usa_discount.json" is the query json file.
The "../data/" is where the csv file and schema lives.

There is also a simple bash program called "run_all_queries.sh" that runs all the queries for you.


To validate these queries honestly I just did it by hand because the query and datasets were not big at all and they all turned out correct.