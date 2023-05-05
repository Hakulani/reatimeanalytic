 
 
 

## Objective:

The objective of this project is to design and implement a data-streaming and real-time analytics system that can be used to analyze college students' food and cooking preferences data from Kaggle. The project involves data cleansing and analytics to be done in real-time.

Datasets:

The project uses the Food Choices dataset available on Kaggle. The dataset contains 106 records and 61 columns.

## System Design:

The system design for the project is as follows:

1. Use Python to write the data source line-by-line from the CSV to SQL database (DB) (source part). The writing duration (d) must be random and between 0 < d < 2 seconds.

2. Connect the SQL DB to a Kafka cluster via topic1.

3. Clean the data in topic1 using ksqlDB and then submit it to topic2.

4. Analyze the data in topic2 using ksqlDB and then submit it to topic3.

5. The source part (NoSQL DB) will incrementally consume the data from topic3

6. Visualize the real-time data from topic3 using tools such as Plotly, etc.

7. Conduct analytics based on your questions

8. Set up and answer three questions:

  - Easy, e.g., simple statistics
  - Medium, e.g., join, windowed, etc.
  - Hard, e.g., analytical insight

## Steps:

1. Create docker file from the ksqlDB tutorial page. 
https://docs.ksqldb.io/en/latest/tutorials/etl/

2. After docker build is complete, run the file create_table.py to create a table inside Postgres, read csv file, and insert into Postgres.

   ```python create_table.py```
   
   You can check Postgres with the following command:

   ```docker exec -it postgres /bin/bash```
   
   ```psql -U postgres-user customers```
   
   Inside Ksql, run the command to check:

   ```SHOW TABLES;```

3. Run the file import_foodcoded.py to import data from the food_coded.csv file.

   ```python import_foodcoded.py```

4. Start Ksql to create the connector.

   ```docker exec -it ksqldb-cli ksql http://ksqldb-server:8088```
   
   Enter the following queries to create a source connector:

 
      ```
      SET 'auto.offset.reset' = 'earliest';
       CREATE SOURCE CONNECTOR customers_reader WITH (
       'connector.class' = 'io.debezium.connector.postgresql.PostgresConnector',
       'database.hostname' = 'postgres',
       'database.port' = '5432',
       'database.user' = 'postgres-user',
       'database.password' = 'postgres-pw',
       'database.dbname' = 'customers',
       'database.server.name' = 'localhost',
       'table.whitelist' = 'public.foodcoded',
       'transforms' = 'unwrap',
       'transforms.unwrap.type' = 'io.debezium.transforms.ExtractNewRecordState',
       'transforms.unwrap.drop.tombstones' = 'false',
       'transforms.unwrap.delete.handling.mode' = 'rewrite'
   );   
   
   ```
 
   Create a Stream topic1 for receiving data:
 
      ```CREATE STREAM foodcoded WITH (kafka_topic = 'localhost.public.foodcoded', value_format = 'avro');```
   
 
   Create a Stream topic2 for cleaning data follow food_clean.sql.

   Create a Stream topic3 for analyzing data follow food_analyze.sql.

   Enter the following queries to create a sink connector:

 
   ```
   CREATE SINK CONNECTOR `mongodb_foodcoded_sink` WITH (
   "connector.class"='com.mongodb.kafka.connect.MongoSinkConnector',
   "key.converter"='org.apache.kafka.connect.storage.StringConverter',
   "value.converter"='io.confluent.connect.avro.AvroConverter',
   "value.converter.schema.registry.url"='http://schema-registry:8081',
   "key.converter.schemas.enable"='false',
   "value.converter.schemas.enable"='true',
   "tasks.max"='1',
   "connection.uri"='mongodb://root:rootpassword@mongodb:27017/admin?readPreference=primary&appname=ksqldbConnect&ssl=false',
   "database"='foodcoded_db',
   "collection"='foodcoded_analyze',
   "topics"='food_analyze'
);```   


 
  6. Data visualization from mongoDB with python 
 Using Dash plotly

python app.py
Dash is running on http://127.0.0.1:8050/

python app2.py

Dash is running on http://127.0.0.1:8050/

  
 
