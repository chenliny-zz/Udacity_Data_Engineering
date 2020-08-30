## Data Pipelines with Airflow

### Description
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The objective of this project is to create high grade data pipelines that are **dynamic** and built from **reusable tasks**, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run **tests against their datasets** after the ETL steps have been executed to catch any discrepancies in the datasets. Project steps include the following:
- create four custom operators to perform tasks such as staging the data, transforming the data, filling the data warehouse, and running validation checks on the data
- implement operators into functional pieces of the data pipeline
- link tasks to create create a coherent and sensible data flow within the pipeline

The source data **resides in S3** and needs to be processed in Sparkify's data warehouse in **Amazon Redshift**. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
![sample_dag](image/example_dag.png)

### Project datasets
Datasets in this project resides in S3. Here are the S3 links for each:
- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

### The Operators
**Stage operator**: The stage operator is able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters specify where in S3 the file is loaded and what is the target table. The parameters are used to distinguish between JSON file. The stage operator also contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

**Fact and Dimension Operators**: The operator is expected to take as input a SQL statement and target database on which to run the query against.

**Data Quality Operator**: The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator will raise an exception and the task should retry and fail eventually. An example test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.
