## Project 2 - Data Modeling with Apache Cassandra

#### Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. There is no easy way to query the data to generate the results, since the data reside in a directory of text files on user activity on the app. The objective is to create an Apache Cassandra database which can create queries on song play data to answer the questions. In this project, I apply data modeling with Apache Cassandra and develop an ETL pipeline using Python.

#### Database design
The dataset used for this project is under ```event_data``` - a directory of text files partitioned by date. Here are examples of file paths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
![image](image/image_event_datafile_new.jpg)

#### The ETL pipeline
1. Implement the logic in Part 1 of the notebook to iterate through each event file in ```event_data``` to process and create a new text file in Python
2. Develop ```CREATE``` and ```INSERT``` statements to load processed records into relevant tables in the data model
3. Test by running ```SELECT``` statements after executing the queries on the database
4. Drop the tables and shutdown the cluster

#### Relevant files
- **Data_modeling_with_apache_cassandra.ipynb**: the main notebook in which all queries have been written.
- **event_data**: the raw data directory containing all event data
- **event_datafile_new.csv**: the final combination of all the event files from the ```event_data``` directory
