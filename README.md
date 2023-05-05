# reatimeanalytic
 

```
 ------------------------
 Step
1. create docker file follow 
https://docs.ksqldb.io/en/latest/tutorials/etl/

2.After docker build complete, run file <create_table.py> to create table inside Postgres
read csv file and insert into Postgres


python import_foodcoded.py


3.Run file <import_foodcoded.py> to import data from <food_coded.csv>

docker-compose up -d
After docker build complete, run file <create_table.py> to create table inside Postgres

python create_table.py
You can check Postgres with command
docker exec -it postgres /bin/bash
psql -U postgres-user customers
Inside Ksql, run command to check
SHOW TABLES;
3. Run file <import_foodcoded.py> to import data from <food_coded.csv>

python import_foodcoded.py
4.Start Ksql to create connector

docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
Enter following queries to create a source connector

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
Create a Stream topic1 for recieve data

CREATE STREAM foodcoded WITH (
    kafka_topic = 'localhost.public.foodcoded',
    value_format = 'avro'
);
Create a Stream topic2 for clean data follow <food_clean.sql>

Create a Stream topic3 for analyze data follow <food_analyze.sql>

Enter following queries to create a sink connector



5. Analyzed data from mongoDB


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
);
 Message
------------------------------------------
 Created connector mongodb_foodcoded_sink
------------------------------------------
6. Data visualization from mongoDB with python 
 Using Dash plotly

python app.py
Dash is running on http://127.0.0.1:8050/
--------------------------
---
version: '2'

services:
  postgres:
    platform: linux/x86_64
    image: debezium/postgres:12
    hostname: postgres
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres-user
      POSTGRES_PASSWORD: postgres-pw
      POSTGRES_DB: customers
    volumes:
      - ./postgres/custom-config.conf:/etc/postgresql/postgresql.conf
    command: postgres -c config_file=/etc/postgresql/postgresql.conf

  elastic:
    platform: linux/x86_64
    image: elasticsearch:7.6.2
    hostname: elastic
    container_name: elastic
    ports:
      - "9200:9200"
      - "9300:9300"
    environment:
      discovery.type: single-node

  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.3.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.3.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - zookeeper
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://broker:9092"

  ksqldb-server:
    image: confluentinc/ksqldb-server:0.28.2
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./confluent-hub-components/:/usr/share/kafka/plugins/"
    environment:
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      KSQL_CONNECT_GROUP_ID: "ksql-connect-cluster"
      KSQL_CONNECT_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.storage.StringConverter"
      KSQL_CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
      KSQL_CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_CONNECT_CONFIG_STORAGE_TOPIC: "_ksql-connect-configs"
      KSQL_CONNECT_OFFSET_STORAGE_TOPIC: "_ksql-connect-offsets"
      KSQL_CONNECT_STATUS_STORAGE_TOPIC: "_ksql-connect-statuses"
      KSQL_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_PLUGIN_PATH: "/usr/share/kafka/plugins"

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.28.2
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
--------------------------


Get the connectors¶
To get started, download the connectors for Postgres, MongoDB, and Elasticsearch to a fresh directory. The easiest way to do this is by using confluent-hub.   https://docs.confluent.io/platform/current/connect/confluent-hub/client.html

Create a directory for your components:
mkdir confluent-hub-components

First, acquire the Postgres Debezium connector:
confluent-hub install --component-dir confluent-hub-components --no-prompt debezium/debezium-connector-postgresql:1.1.0

Likewise for the MongoDB Debezium connector:
confluent-hub install --component-dir confluent-hub-components --no-prompt debezium/debezium-connector-mongodb:1.1.0

And finally, the Elasticsearch connector:
confluent-hub install --component-dir confluent-hub-components --no-prompt confluentinc/kafka-connect-elasticsearch:10.0.1

มี folder  confluent-hub-components
ref : Debezium Connector for MongoDB
https://debezium.io/documentation/reference/1.1/connectors/mongodb.html
 


docker compose up -d
[+] Running 10/10
 - mongodb Pulled                                                                                                 17.7s
   - 1bc677758ad7 Pull complete                                                                                    3.2s
   - 7eb83bb7be98 Pull complete                                                                                    3.3s
   - e95121721c4c Pull complete                                                                                    3.4s
   - 799041b403ca Pull complete                                                                                    3.5s
   - 1828e70ef29a Pull complete                                                                                    3.6s
   - 8e3781beae9e Pull complete                                                                                    3.6s
   - 5d5753162333 Pull complete                                                                                    3.6s
   - 44dd404b40f4 Pull complete                                                                                   11.9s
   - 44599c9d5d1b Pull complete                                                                                   11.9s
[+] Running 9/9
 - Network main_default       Created                                                                              0.9s
 - Container zookeeper        Started                                                                              4.3s
 - Container postgres         Started                                                                              4.6s
 - Container mongodb          Started                                                                              3.8s
 - Container elastic          Started                                                                              5.7s
 - Container broker           Started                                                                              5.5s
 - Container schema-registry  Started                                                                              7.3s
 - Container ksqldb-server    Started                                                                              9.3s
 - Container ksqldb-cli       Started                                                                             11.3s

D:\DADS6005\Quiz2\main>
Create table to Postgres with python 
python create_table.py
create table
Check data 
 

## Config  ##
dbServerName    = "localhost"
dbUser          = "postgres-user"
dbPassword      = "postgres-pw"
dbName          = "customers"
TABLE   foodcoded 



docker exec -it postgres /bin/bash
psql -U postgres-user customers
customers=# select * from foodcoded;
 gpa | gender | breakfast | calories_chicken | calories_day | calories_scone | coffee | comfort_food | comfort_food_reas
ons | comfort_food_reasons_coded | cook | comfort_food_reasons_coded1 | cuisine | diet_current | diet_current_coded | dr
ink | eating_changes | eating_changes_coded | eating_changes_coded1 | eating_out | employment | ethnic_food | exercise |
 father_education | father_profession | fav_cuisine | fav_cuisine_coded | fav_food | food_childhood | fries | fruit_day
| grade_level | greek_food | healthy_feeling | healthy_meal | ideal_diet | ideal_diet_coded | income | indian_food | ita
lian_food | life_rewarding | marital_status | meals_dinner_friend | mother_education | mother_profession | nutritional_c
heck | on_off_campus | parents_cook | pay_meal_out | persian_food | self_perception_weight | soup | sports | thai_food |
 tortilla_calories | turkey_calories | type_sports | veggies_day | vitamins | waffle_calories | weight

read csv file and insert into Postgres


python import_foodcoded.py

import row  1
import row  2
import row  3
import row  4
import row  5
import row  6
import row  7
import row  8
import row  9
import row  10
import row  11

docker exec -it postgres /bin/bash
psql -U postgres-user customers
customers=# select * from foodcoded;

 gpa  | gender | breakfast | calories_chicken | calories_day | calories_scone | coffee |                       comfort_food                        |                                                       comfort_food_reasons
                                        | comfort_food_reasons_coded | cook | comfort_food_reasons_coded1 | cuisine |
                                                                                                      diet_current
                                                                                                    | diet_current_coded | drink |                                                                                       eating_changes
                                                                              | eating_changes_coded | eating_changes_coded1 | eating_out | employment | ethnic_food | exercise | father_education |      father_profession       |
 fav_cuisine              | fav_cuisine_coded | fav_food |                food_childhood                | fries | fruit_day | grade_level | greek_food | healthy_feeling |                                                   healthy_meal
                                            |                                                                   ideal_diet                                                                   | ideal_diet_coded | income | indian_food | italian_food | life_rewarding | marital_status |                                                  meals_dinner_friend
                                         | mother_education |     mother_profession     | nutritional_check | on_off_campus | parents_cook | pay_meal_out | persian_food | self_perception_weight | soup | sports | thai_food | tortilla_calories | turkey_calories |      type_sports       | veggies_day | vitamins | waffle_calories |          weight
-------+--------+-----------+------------------+--------------+----------------+--------+-----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+----------------------------+------+-----------------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+-----------------------+------------+------------+-------------+----------+------------------+------------------------------+---------------------------------------+-------------------+----------+----------------------------------------------+-------+-----------+-------------+------------+-----------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------+------------------+--------+-------------+--------------+----------------+----------------+------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------+-------------------+---------------+--------------+--------------+--------------+------------------------+------+--------+-----------+-------------------+-----------------+------------------------+-------------+----------+-----------------+--------------------------
 2.4   |      2 |         1 |              430 |              |            315 |      1 | none
                            | we dont have comfort
                                        |                          9 |    2 |                             |         | eat good and exercise
                                                                                                    |                  1 |     1 | eat faster
                                                                              |                    1 |
   1 |          3 |          3 |           1 |        1 |                5 | profesor                     | Arabic cuisine                        |                 3 |        1 | rice  and chicken                            |     2 |
  5 |           2 |          5 |               2 | looks not oily
                                            | being healthy
                                                                     |                8 |      5 |           5 |
    5 |              1 |              1 | rice, chicken,  soup
                                         |                1 | unemployed                |                 5 |
  1 |            1 |            2 |            5 |                      3 |    1 |      1 |         1 |              1165 |             345 | car racing             |           5 |        1 |            1315 | 187
 3.654 |      1 |         1 |              610 |            3 |            420 |      2 | chocolate, chips, ice cream
                            | Stress, bored, anger
                                        |                          1 |    3 |                             |       1 | I
more

ksqldb docker


docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
 
OpenJDK 64-Bit Server VM warning: Option UseConcMarkSweepGC was deprecated in version 9.0 and will likely be removed in a future release.

                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =        The Database purpose-built       =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2022 Confluent Inc.

CLI v0.28.2, Server v0.28.2 located at http://ksqldb-server:8088
Server Status: RUNNING

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

SET 'auto.offset.reset' = 'earliest';
Successfully changed local property 'auto.offset.reset' to 'earliest'. Use the UNSET command to revert your change.

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

Message
------------------------------------
 Created connector CUSTOMERS_READER
------------------------------------

SHOW CONNECTORS;
 Connector Name   | Type   | Class                                              | Status
--------------------------------------------------------------------------------------------------------------
 CUSTOMERS_READER | SOURCE | io.debezium.connector.postgresql.PostgresConnector | RUNNING (1/1 tasks RUNNING)
--------------------------------------------------------------------------------------------------------------
Create a Stream topic "foodcoded" for recieve data


CREATE STREAM foodcoded WITH (
    kafka_topic = 'localhost.public.foodcoded',
    value_format = 'avro'
);
 
Message
----------------
 Stream created
----------------


 Stream Name         | Kafka Topic                 | Key Format | Value Format | Windowed
------------------------------------------------------------------------------------------
 FOODCODED           | localhost.public.foodcoded  | KAFKA      | AVRO         | false
 KSQL_PROCESSING_LOG | default_ksql_processing_log | KAFKA      | JSON         | false
------------------------------------------------------------------------------------------

Show data from table


SELECT * from foodcoded 
EMIT CHANGES;


|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |fruit|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |s, ca|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |rbohy|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |drate|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |s, an|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |d fat|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |.    |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |


Create a Stream topic "foodcoded_clean" for clean data
 
CREATE STREAM foodcoded_clean WITH (kafka_topic = 'foodcoded_clean') AS 


      SELECT
            (CASE WHEN ((foodcoded.GPA = '') OR (foodcoded.GPA = 'Personal') OR (foodcoded.GPA = 'Unknown')) THEN null
             ELSE CAST(REGEXP_REPLACE(foodcoded.GPA, '[a-zA-Z]+', '') AS DOUBLE) END) GPA,
            (CASE WHEN (CAST(foodcoded.Gender AS VARCHAR) = '') THEN null ELSE foodcoded.Gender END) Gender,
            (CASE WHEN (CAST(foodcoded.calories_day AS VARCHAR) = '') THEN null ELSE foodcoded.calories_day END) calories_day,
            (CASE WHEN (CAST(foodcoded.comfort_food_reasons_coded1 AS VARCHAR) = '') THEN null ELSE foodcoded.comfort_food_reasons_coded1 END) comfort_food_reasons_coded,
            (CASE WHEN (CAST(foodcoded.cook AS VARCHAR) = '') THEN null ELSE foodcoded.cook END) cook,
            (CASE WHEN (CAST(foodcoded.cuisine AS VARCHAR) = '') THEN null ELSE foodcoded.cuisine END) cuisine,
            (CASE WHEN (CAST(foodcoded.diet_current_coded AS VARCHAR) = '') THEN null ELSE foodcoded.diet_current_coded END) diet_current_coded,
            (CASE WHEN (CAST(foodcoded.eating_out AS VARCHAR) = '') THEN null ELSE foodcoded.eating_out END) eating_out,
            (CASE WHEN (CAST(foodcoded.employment AS VARCHAR) = '') THEN null ELSE foodcoded.employment END) employment,
            (CASE WHEN (CAST(foodcoded.ethnic_food AS VARCHAR) = '') THEN null ELSE foodcoded.ethnic_food END) ethnic_food,
            (CASE WHEN (CAST(foodcoded.exercise AS VARCHAR) = '') THEN null ELSE foodcoded.exercise END) exercise,
            (CASE WHEN (CAST(foodcoded.fav_cuisine AS VARCHAR) = '') THEN null ELSE foodcoded.fav_cuisine END) fav_cuisine,
            (CASE WHEN (CAST(foodcoded.fav_cuisine_coded AS VARCHAR) = '') THEN null ELSE foodcoded.fav_cuisine_coded END) fav_cuisine_coded,
            (CASE WHEN (CAST(foodcoded.fav_food AS VARCHAR) = '') THEN null ELSE foodcoded.fav_food END) fav_food,
            (CASE WHEN (CAST(foodcoded.food_childhood AS VARCHAR) = '') THEN null ELSE foodcoded.food_childhood END) food_childhood,
            (CASE WHEN (CAST(foodcoded.fruit_day AS VARCHAR) = '') THEN null ELSE foodcoded.fruit_day END) fruit_day,
            (CASE WHEN (CAST(foodcoded.greek_food AS VARCHAR) = '') THEN null ELSE foodcoded.greek_food END) greek_food,
            (CASE WHEN (CAST(foodcoded.healthy_feeling AS VARCHAR) = '') THEN null ELSE foodcoded.healthy_feeling END) healthy_feeling,
            (CASE WHEN (CAST(foodcoded.income AS VARCHAR) = '') THEN null ELSE foodcoded.income END) income,
            (CASE WHEN (CAST(foodcoded.indian_food AS VARCHAR) = '') THEN null ELSE foodcoded.indian_food END) indian_food,
            (CASE WHEN (CAST(foodcoded.italian_food AS VARCHAR) = '') THEN null ELSE foodcoded.italian_food END) italian_food,
            (CASE WHEN (CAST(foodcoded.marital_status AS VARCHAR) = '') THEN null ELSE foodcoded.marital_status END) marital_status,
            (CASE WHEN (CAST(foodcoded.nutritional_check AS VARCHAR) = '') THEN null ELSE foodcoded.nutritional_check END) nutritional_check,
            (CASE WHEN (CAST(foodcoded.on_off_campus AS VARCHAR) = '') THEN null ELSE foodcoded.on_off_campus END) on_off_campus,
            (CASE WHEN (CAST(foodcoded.parents_cook AS VARCHAR) = '') THEN null ELSE foodcoded.parents_cook END) parents_cook,
            (CASE WHEN (CAST(foodcoded.pay_meal_out AS VARCHAR) = '') THEN null ELSE foodcoded.pay_meal_out END) pay_meal_out,
            (CASE WHEN (CAST(foodcoded.persian_food AS VARCHAR) = '') THEN null ELSE foodcoded.persian_food END) persian_food,
            (CASE WHEN (CAST(foodcoded.self_perception_weight AS VARCHAR) = '') THEN null ELSE foodcoded.self_perception_weight END) self_perception_weight,
            (CASE WHEN (CAST(foodcoded.sports AS VARCHAR) = '') THEN null ELSE foodcoded.sports END) sports,
            (CASE WHEN (CAST(foodcoded.thai_food AS VARCHAR) = '') THEN null ELSE foodcoded.thai_food END) thai_food,
            (CASE WHEN (CAST(foodcoded.veggies_day AS VARCHAR) = '') THEN null ELSE foodcoded.veggies_day END) veggies_day,
            (CASE WHEN (CAST(foodcoded.vitamins AS VARCHAR) = '') THEN null ELSE foodcoded.vitamins END) vitamins,
            (CASE WHEN (foodcoded.weight = '') THEN null
             ELSE CAST(REGEXP_REPLACE(foodcoded.weight, '[^0-9]+', '') AS INT) END) weight
      FROM foodcoded
      EMIT CHANGES;

 Message
-----------------------------------------------
 Created query with ID CSAS_FOODCODED_CLEAN_11
-----------------------------------------------

ksql> SHOW STREAMS;
|     |     |     |     |     |     |     |     |     |     |     |uisin|     |     |opcor|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |e    |     |     |n    |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.4  |1    |null |null |3    |null |2    |4    |3    |4    |2    |Chine|4    |2    |Fry C|5    |1    |5    |1    |3    |5    |1    |2    |1    |2    |3    |1    |4    |2    |5    |5    |1    |170  |
|     |     |     |     |     |     |     |     |     |     |     |se   |     |     |hicke|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |n, Ri|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |ce Ve|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |getab|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |le   |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.77 |1    |null |null |3    |4    |1    |2    |2    |4    |null |Vietn|4    |3    |Noodl|3    |2    |9    |2    |2    |4    |1    |2    |1    |1    |2    |2    |4    |2    |5    |3    |1    |113  |
|     |     |     |     |     |     |     |     |     |     |     |amese|     |     |e, Wi|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     | cuis|     |     |ngs, |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |ine  |     |     |and T|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |irami|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |su   |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.63 |1    |3    |null |3    |1    |2    |2    |2    |4    |2    |Ameri|5    |1    |Chine|5    |3    |5    |3    |3    |5    |2    |2    |3    |3    |4    |2    |4    |2    |4    |5    |2    |140  |
|     |     |     |     |     |     |     |     |     |     |     |can  |     |     |se   |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.2  |2    |3    |null |2    |5    |2    |2    |2    |5    |2    |India|8    |3    |pizza|5    |5    |7    |2    |5    |5    |2    |2    |1    |1    |3    |5    |4    |1    |5    |5    |2    |185  |
|     |     |     |     |     |     |     |     |     |     |     |n    |     |     |, bur|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |ger, |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |pasta|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.5  |1    |4    |null |3    |1    |2    |2    |1    |4    |2    |Itali|1    |1    |Strom|5    |5    |5    |4    |3    |5    |1    |5    |3    |1    |4    |3    |4    |1    |5    |5    |1    |156  |
|     |     |     |     |     |     |     |     |     |     |     |an   |     |     |boli |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |Mac a|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |nd Ch|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |eese |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |and P|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |izza |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.0  |1    |2    |null |3    |null |2    |4    |3    |3    |2    |Mexic|2    |1    |Isomb|4    |1    |5    |2    |5    |5    |1    |3    |1    |3    |4    |1    |4    |null |4    |5    |2    |180  |
|     |     |     |     |     |     |     |     |     |     |     |an Fo|     |     |e , P|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |od   |     |     |lanta|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |ins a|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |nd Ug|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |ali  |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.882|1    |null |null |3    |null |2    |3    |3    |5    |2    |Korea|4    |1    |Rice |4    |5    |6    |2    |5    |3    |1    |3    |1    |2    |4    |5    |4    |2    |5    |4    |2    |120  |
|     |     |     |     |     |     |     |     |     |     |     |n    |     |     |and p|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |otato|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.0  |2    |4    |null |3    |1    |1    |5    |2    |2    |1    |Itali|1    |3    |pizza|5    |1    |1    |4    |1    |5    |1    |4    |1    |2    |3    |1    |2    |2    |1    |3    |1    |135  |
|     |     |     |     |     |     |     |     |     |     |     |an   |     |     | and |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |spagh|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |etti |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|3.9  |1    |null |null |null |3    |1    |1    |2    |3    |2    |HISPA|2    |1    |rice,|3    |2    |3    |5    |2    |3    |2    |5    |1    |3    |3    |2    |3    |2    |2    |4    |2    |135  |
|     |     |     |     |     |     |     |     |     |     |     |NIC C|     |     | bean|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |UISIN|     |     |s, an|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |E.   |     |     |d chi|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |cken |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |/ piz|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |za/ t|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |ender|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
|     |     |     |     |     |     |     |     |     |     |     |     |     |     |s    |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |


code plan
เว้นไว้

create analyze  topic

CREATE STREAM foodcoded_analyze WITH (kafka_topic = 'foodcoded_analyze') AS 
      SELECT
            foodcoded_clean.GPA,
            (CASE WHEN (GPA = 4)   THEN 'A'
                  WHEN (GPA >= 3.5 and GPA < 4)   THEN 'B+'
                  WHEN (GPA >= 3   and GPA < 3.5) THEN 'B'
                  WHEN (GPA >= 2.5 and GPA < 3)   THEN 'C+'
                  WHEN (GPA >= 2   and GPA < 2.5) THEN 'C'
                  WHEN (GPA >= 0   and GPA < 2)   THEN 'F'
             ELSE null END) Grade,
            foodcoded_clean.Gender,
            (CASE WHEN (foodcoded_clean.Gender = 1) THEN 'Female'
                  WHEN (foodcoded_clean.Gender = 2) THEN 'Male'
             ELSE null END) Gender_desc,
            foodcoded_clean.calories_day,
            (CASE WHEN (foodcoded_clean.calories_day = 1) THEN 'i dont know how many calories i should consume'
                  WHEN (foodcoded_clean.calories_day = 2) THEN 'it is not at all important'
                  WHEN (foodcoded_clean.calories_day = 3) THEN 'it is moderately important'
                  WHEN (foodcoded_clean.calories_day = 4) THEN 'it is very important'
             ELSE null END) calories_day_desc,
            foodcoded_clean.comfort_food_reasons_coded,
            (CASE WHEN (foodcoded_clean.comfort_food_reasons_coded = 1) THEN 'stress'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 2) THEN 'boredom'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 3) THEN 'depression/sadness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 4) THEN 'hunger'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 5) THEN 'laziness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 6) THEN 'cold weather'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 7) THEN 'happiness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 8) THEN 'watching tv'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 9) THEN 'none'
             ELSE null END) comfort_food_reasons_coded_desc,
            foodcoded_clean.cook,
            (CASE WHEN (foodcoded_clean.cook = 1) THEN 'Every day'
                  WHEN (foodcoded_clean.cook = 2) THEN 'A couple of times a week'
                  WHEN (foodcoded_clean.cook = 3) THEN 'Whenever I can, but that is not very often'
                  WHEN (foodcoded_clean.cook = 4) THEN 'I only help a little during holidays'
                  WHEN (foodcoded_clean.cook = 5) THEN 'Never, I really do not know my way around a kitchen'
             ELSE null END) cook_desc,
            foodcoded_clean.cuisine,
            (CASE WHEN (foodcoded_clean.cuisine = 1) THEN 'American'
                  WHEN (foodcoded_clean.cuisine = 2) THEN 'Mexican.Spanish'
                  WHEN (foodcoded_clean.cuisine = 3) THEN 'Korean/Asian'
                  WHEN (foodcoded_clean.cuisine = 4) THEN 'Indian'
                  WHEN (foodcoded_clean.cuisine = 5) THEN 'American inspired international dishes'
                  WHEN (foodcoded_clean.cuisine = 6) THEN 'other'
             ELSE null END) cuisine_desc,
            foodcoded_clean.diet_current_coded,
            (CASE WHEN (foodcoded_clean.diet_current_coded = 1) THEN 'healthy/balanced/moderated/'
                  WHEN (foodcoded_clean.diet_current_coded = 2) THEN 'unhealthy/cheap/too much/random/'
                  WHEN (foodcoded_clean.diet_current_coded = 3) THEN 'the same thing over and over'
                  WHEN (foodcoded_clean.diet_current_coded = 4) THEN 'unclear'
             ELSE null END) diet_current_coded_desc,
            foodcoded_clean.eating_out,
            (CASE WHEN (foodcoded_clean.eating_out = 1) THEN 'Never'
                  WHEN (foodcoded_clean.eating_out = 2) THEN '1-2 times'
                  WHEN (foodcoded_clean.eating_out = 3) THEN '2-3 times'
                  WHEN (foodcoded_clean.eating_out = 4) THEN '3-5 times'
                  WHEN (foodcoded_clean.eating_out = 5) THEN 'every day'
             ELSE null END) eating_out_desc,
            foodcoded_clean.employment,
            (CASE WHEN (foodcoded_clean.employment = 1) THEN 'yes full time'
                  WHEN (foodcoded_clean.employment = 2) THEN 'yes part time'
                  WHEN (foodcoded_clean.employment = 3) THEN 'no'
                  WHEN (foodcoded_clean.employment = 4) THEN 'other'
             ELSE null END) employment_desc,
            foodcoded_clean.ethnic_food,
            (CASE WHEN (foodcoded_clean.ethnic_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.ethnic_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.ethnic_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.ethnic_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.ethnic_food = 5) THEN 'very likely'
             ELSE null END) ethnic_food_desc,
            foodcoded_clean.exercise,
            (CASE WHEN (foodcoded_clean.exercise = 1) THEN 'Everyday'
                  WHEN (foodcoded_clean.exercise = 2) THEN 'Twice or three times per week'
                  WHEN (foodcoded_clean.exercise = 3) THEN 'Once a week'
                  WHEN (foodcoded_clean.exercise = 4) THEN 'Sometimes'
                  WHEN (foodcoded_clean.exercise = 4) THEN 'Never'
            ELSE null END) exercise_desc,
            foodcoded_clean.fav_cuisine,
            foodcoded_clean.fav_cuisine_coded,
            (CASE WHEN (foodcoded_clean.fav_cuisine_coded = 0) THEN 'none'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 1) THEN 'Italian/French/greek'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 2) THEN 'Spanish/mexican'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 3) THEN 'Arabic/Turkish'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 4) THEN 'asian/chineses/thai/nepal'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 5) THEN 'American'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 6) THEN 'African'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 7) THEN 'Jamaican'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 8) THEN 'indian'
             ELSE null END) fav_cuisine_coded_desc,
            foodcoded_clean.fav_food,
            (CASE WHEN (foodcoded_clean.fav_food = 1) THEN 'cooked at home'
                  WHEN (foodcoded_clean.fav_food = 2) THEN 'store bought'
                  WHEN (foodcoded_clean.fav_food = 3) THEN 'both bought at store and cooked at home'
             ELSE null END) fav_food_desc,
            foodcoded_clean.food_childhood,
            (SUBSTRING(foodcoded_clean.food_childhood, 1, INSTR(foodcoded_clean.food_childhood, ','))) food_childhood_split,
            foodcoded_clean.fruit_day,
            (CASE WHEN (foodcoded_clean.fruit_day = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.fruit_day = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.fruit_day = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.fruit_day = 4) THEN 'likely'
                  WHEN (foodcoded_clean.fruit_day = 5) THEN 'very likely'
             ELSE null END) fruit_day_desc,
            foodcoded_clean.greek_food,
            (CASE WHEN (foodcoded_clean.greek_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.greek_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.greek_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.greek_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.greek_food = 5) THEN 'very likely'
             ELSE null END) greek_food_desc,
            foodcoded_clean.healthy_feeling,
            foodcoded_clean.income,
            (CASE WHEN (foodcoded_clean.income = 1) THEN 'less than $15,000'
                  WHEN (foodcoded_clean.income = 2) THEN '$15,001 to $30,000'
                  WHEN (foodcoded_clean.income = 3) THEN '$30,001 to $50,000'
                  WHEN (foodcoded_clean.income = 4) THEN '$50,001 to $70,000'
                  WHEN (foodcoded_clean.income = 5) THEN '$70,001 to $100,000'
                  WHEN (foodcoded_clean.income = 6) THEN 'higher than $100,000'
             ELSE null END) income_desc,
            foodcoded_clean.indian_food,
            (CASE WHEN (foodcoded_clean.indian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.indian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.indian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.indian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.indian_food = 5) THEN 'very likely'
            ELSE null END) indian_food_desc,
            foodcoded_clean.italian_food,
            (CASE WHEN (foodcoded_clean.italian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.italian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.italian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.italian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.italian_food = 5) THEN 'very likely'
             ELSE null END) italian_food_desc,
            foodcoded_clean.marital_status,
            (CASE WHEN (foodcoded_clean.marital_status = 1) THEN 'Single'
                  WHEN (foodcoded_clean.marital_status = 2) THEN 'In a relationship'
                  WHEN (foodcoded_clean.marital_status = 3) THEN 'Cohabiting'
                  WHEN (foodcoded_clean.marital_status = 4) THEN 'Married'
                  WHEN (foodcoded_clean.marital_status = 5) THEN 'Divorced'
                  WHEN (foodcoded_clean.marital_status = 6) THEN 'Widowed'
             ELSE null END) marital_status_desc,
            foodcoded_clean.nutritional_check,
            (CASE WHEN (foodcoded_clean.nutritional_check = 1) THEN 'never'
                  WHEN (foodcoded_clean.nutritional_check = 2) THEN 'on certain products only'
                  WHEN (foodcoded_clean.nutritional_check = 3) THEN 'very rarely'
                  WHEN (foodcoded_clean.nutritional_check = 4) THEN 'on most products'
                  WHEN (foodcoded_clean.nutritional_check = 5) THEN 'on everything'
            ELSE null END) nutritional_check_desc,
            foodcoded_clean.on_off_campus,
            (CASE WHEN (foodcoded_clean.on_off_campus = 1) THEN 'On campus'
                  WHEN (foodcoded_clean.on_off_campus = 2) THEN 'Rent out of campus'
                  WHEN (foodcoded_clean.on_off_campus = 3) THEN 'Live with my parents and commute'
                  WHEN (foodcoded_clean.on_off_campus = 4) THEN 'Own my own house'
             ELSE null END) on_off_campus_desc,
            foodcoded_clean.parents_cook,
            (CASE WHEN (foodcoded_clean.parents_cook = 1) THEN 'Almost everyday'
                  WHEN (foodcoded_clean.parents_cook = 2) THEN '2-3 times a week'
                  WHEN (foodcoded_clean.parents_cook = 3) THEN '1-2 times a week'
                  WHEN (foodcoded_clean.parents_cook = 4) THEN 'on holidays only'
                  WHEN (foodcoded_clean.parents_cook = 5) THEN 'never'
             ELSE null END) parents_cook_desc,
            foodcoded_clean.pay_meal_out,
            (CASE WHEN (foodcoded_clean.pay_meal_out = 1) THEN 'up to $5.00'
                  WHEN (foodcoded_clean.pay_meal_out = 2) THEN '$5.01 to $10.00'
                  WHEN (foodcoded_clean.pay_meal_out = 3) THEN '$10.01 to $20.00'
                  WHEN (foodcoded_clean.pay_meal_out = 4) THEN '$20.01 to $30.00'
                  WHEN (foodcoded_clean.pay_meal_out = 5) THEN '$30.01 to $40.00'
                  WHEN (foodcoded_clean.pay_meal_out = 6) THEN 'more than $40.01'
             ELSE null END) pay_meal_out_desc,
            foodcoded_clean.persian_food,
            (CASE WHEN (foodcoded_clean.persian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.persian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.persian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.persian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.persian_food = 5) THEN 'very likely'
             ELSE null END) persian_food_desc,
            foodcoded_clean.self_perception_weight,
            (CASE WHEN (foodcoded_clean.self_perception_weight = 1) THEN 'slim'
                  WHEN (foodcoded_clean.self_perception_weight = 2) THEN 'very fit'
                  WHEN (foodcoded_clean.self_perception_weight = 3) THEN 'just right'
                  WHEN (foodcoded_clean.self_perception_weight = 4) THEN 'slightly overweight'
                  WHEN (foodcoded_clean.self_perception_weight = 5) THEN 'overweight'
                  WHEN (foodcoded_clean.self_perception_weight = 6) THEN 'i dont think myself in these terms'
             ELSE null END) self_perception_weight_desc,
            foodcoded_clean.sports,
            (CASE WHEN (foodcoded_clean.sports = 1) THEN 'Yes'
                  WHEN (foodcoded_clean.sports = 2) THEN 'No'
             ELSE null END) sports_desc,
            foodcoded_clean.thai_food,
            (CASE WHEN (foodcoded_clean.thai_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.thai_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.thai_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.thai_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.thai_food = 5) THEN 'very likely'
             ELSE null END) thai_food_desc,
            foodcoded_clean.veggies_day,
            (CASE WHEN (foodcoded_clean.veggies_day = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.veggies_day = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.veggies_day = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.veggies_day = 4) THEN 'likely'
                  WHEN (foodcoded_clean.veggies_day = 5) THEN 'very likely'
             ELSE null END) veggies_day_desc,
            foodcoded_clean.vitamins,
            (CASE WHEN (foodcoded_clean.vitamins = 1) THEN 'Yes'
                  WHEN (foodcoded_clean.vitamins = 2) THEN 'No'
             ELSE null END) vitamins_desc,
            foodcoded_clean.weight

      FROM foodcoded_clean
      EMIT CHANGES;

Message
-------------------------------------------------
 Created query with ID CSAS_FOODCODED_ANALYZE_13
-------------------------------------------------

PRINT foodcoded_analyze from beginning;
------------------------------------
"American", "DIET_CURRENT_CODED": 1, "DIET_CURRENT_CODED_DESC": "healthy/balanced/moderated/", "EATING_OUT": 5, "EATING_OUT_DESC": "every day", "EMPLOYMENT": 2, "EMPLOYMENT_DESC": "yes part time", "ETHNIC_FOOD": 2, "ETHNIC_FOOD_DESC": "unlikely", "EXERCISE": 1, "EXERCISE_DESC": "Everyday", "FAV_CUISINE": "Italian", "FAV_CUISINE_CODED": 1, "FAV_CUISINE_CODED_DESC": "Italian/French/greek", "FAV_FOOD": 3, "FAV_FOOD_DESC": "both bought at store and cooked at home", "FOOD_CHILDHOOD": "pizza and spaghetti ", "FOOD_CHILDHOOD_SPLIT": "", "FRUIT_DAY": 5, "FRUIT_DAY_DESC": "very likely", "GREEK_FOOD": 1, "GREEK_FOOD_DESC": "very unlikely", "HEALTHY_FEELING": 1, "INCOME": 4, "INCOME_DESC": "$50,001 to $70,000", "INDIAN_FOOD": 1, "INDIAN_FOOD_DESC": "very unlikely", "ITALIAN_FOOD": 5, "ITALIAN_FOOD_DESC": "very likely", "MARITAL_STATUS": 1, "MARITAL_STATUS_DESC": "Single", "NUTRITIONAL_CHECK": 4, "NUTRITIONAL_CHECK_DESC": "on most products", "ON_OFF_CAMPUS": 1, "ON_OFF_CAMPUS_DESC": "On campus", "PARENTS_COOK": 2, "PARENTS_COOK_DESC": "2-3 times a week", "PAY_MEAL_OUT": 3, "PAY_MEAL_OUT_DESC": "$10.01 to $20.00", "PERSIAN_FOOD": 1, "PERSIAN_FOOD_DESC": "very unlikely", "SELF_PERCEPTION_WEIGHT": 2, "SELF_PERCEPTION_WEIGHT_DESC": "very fit", "SPORTS": 2, "SPORTS_DESC": "No", "THAI_FOOD": 1, "THAI_FOOD_DESC": "very unlikely", "VEGGIES_DAY": 3, "VEGGIES_DAY_DESC": "neutral", "VITAMINS": 1, "VITAMINS_DESC": "Yes", "WEIGHT": 135}, partition: 0
rowtime: 2023/05/05 12:00:27.222 Z, key: <null>, value: {"GPA": 3.9, "GRADE": "B+", "GENDER": 1, "GENDER_DESC": "Female", "CALORIES_DAY": null, "CALORIES_DAY_DESC": null, "COMFORT_FOOD_REASONS_CODED": null, "COMFORT_FOOD_REASONS_CODED_DESC": null, "COOK": null, "COOK_DESC": null, "CUISINE": 3, "CUISINE_DESC": "Korean/Asian", "DIET_CURRENT_CODED": 1, "DIET_CURRENT_CODED_DESC": "healthy/balanced/moderated/", "EATING_OUT": 1, "EATING_OUT_DESC": "Never", "EMPLOYMENT": 2, "EMPLOYMENT_DESC": "yes part time", "ETHNIC_FOOD": 3, "ETHNIC_FOOD_DESC": "neutral", "EXERCISE": 2, "EXERCISE_DESC": "Twice or three times per week", "FAV_CUISINE": "HISPANIC CUISINE.", "FAV_CUISINE_CODED": 2, "FAV_CUISINE_CODED_DESC": "Spanish/mexican", "FAV_FOOD": 1, "FAV_FOOD_DESC": "cooked at home", "FOOD_CHILDHOOD": "rice, beans, and chicken / pizza/ tenders", "FOOD_CHILDHOOD_SPLIT": "rice,", "FRUIT_DAY": 3, "FRUIT_DAY_DESC": "neutral", "GREEK_FOOD": 2, "GREEK_FOOD_DESC": "unlikely", "HEALTHY_FEELING": 3, "INCOME": 5, "INCOME_DESC": "$70,001 to $100,000", "INDIAN_FOOD": 2, "INDIAN_FOOD_DESC": "unlikely", "ITALIAN_FOOD": 3, "ITALIAN_FOOD_DESC": "neutral", "MARITAL_STATUS": 2, "MARITAL_STATUS_DESC": "In a relationship", "NUTRITIONAL_CHECK": 5, "NUTRITIONAL_CHECK_DESC": "on everything", "ON_OFF_CAMPUS": 1, "ON_OFF_CAMPUS_DESC": "On campus", "PARENTS_COOK": 3, "PARENTS_COOK_DESC": "1-2 times a week", "PAY_MEAL_OUT": 3, "PAY_MEAL_OUT_DESC": "$10.01 to $20.00", "PERSIAN_FOOD": 2, "PERSIAN_FOOD_DESC": "unlikely", "SELF_PERCEPTION_WEIGHT": 3, "SELF_PERCEPTION_WEIGHT_DESC": "just right", "SPORTS": 2, "SPORTS_DESC": "No", "THAI_FOOD": 2, "THAI_FOOD_DESC": "unlikely", "VEGGIES_DAY": 4, "VEGGIES_DAY_DESC": "likely", "VITAMINS": 2, "VITAMINS_DESC": "No", "WEIGHT": 135}, partition: 0
ksql> SHOW STREAMS;

 Stream Name         | Kafka Topic                 | Key Format | Value Format | Windowed
------------------------------------------------------------------------------------------
 FOODCODED           | localhost.public.foodcoded  | KAFKA      | AVRO         | false
 FOODCODED_ANALYZE   | foodcoded_analyze           | KAFKA      | AVRO         | false
 FOODCODED_CLEAN     | foodcoded_clean             | KAFKA      | AVRO         | false
 KSQL_PROCESSING_LOG | default_ksql_processing_log | KAFKA      | JSON         | false
------------------------------------------------------------------------------------------

Check data 

ksql> SELECT * FROM FOODCODED_CLEAN;
PRINT foodcoded_clean from beginning ;




SELECT * FROM foodcoded_analyze
EMIT CHANGES;


ksql> SELECT * FROM  FOODCODED_ANALYZE
>EMIT CHANGES;
PRINT foodcoded_analyze from beginning ;

 create a sink connector


Next, create a JSON configuration file for the MongoDB sink connector, e.g., mongodb-sink-connector.json:

{
  "name": "mongodb-sink-connector",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "foodcoded_analyze",
    "connection.uri": "mongodb://root:rootpassword@mongodb:27017",
    "database": "foodcoded_db",
    "collection": "foodcoded_analyze",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}


Then, run the following command from your host machine to create the MongoDB sink connector:

curl -X POST -H "Content-Type: application/json" -d @mongodb-sink-connector.json http://localhost:8083/connectors


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
   "topics"='foodcoded_analyze'
);
 Message
------------------------------------------
 Created connector mongodb_foodcoded_sink
------------------------------------------

create app.py เพื่อทำการอ่าน data from mongoDB



 ```
