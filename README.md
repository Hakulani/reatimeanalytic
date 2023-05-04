# reatimeanalytic
reatimeanalytic
```

cmd 

docker compose up -d
docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088
SET 'auto.offset.reset'='earliest'; 
RUN SCRIPT '/etc/sql/all.sql';


open new terminal

docker exec -ti postgres psql -c "select * from food"

open new terminal
docker-compose exec elasticsearch curl -XGET "localhost:9200/food/_search?format=json&prett

ksql> SHOW TOPICS;

 Kafka Topic           | Partitions | Partition Replicas
---------------------------------------------------------
 food                  | 1          | 1
 ksql-connect-configs  | 1          | 1
 ksql-connect-offsets  | 25         | 1
 ksql-connect-statuses | 5          | 1
---------------------------------------------------------
ksql> LIST TOPICS;

 Kafka Topic           | Partitions | Partition Replicas
---------------------------------------------------------
 food                  | 1          | 1
 ksql-connect-configs  | 1          | 1
 ksql-connect-offsets  | 25         | 1
 ksql-connect-statuses | 5          | 1
---------------------------------------------------------
ksql> SHOW CONNECTORS;

 Connector Name     | Type   | Class                                                         | Status

------------------------------------------------------------------------------------------------------------------------
 postgres-source    | SOURCE | io.confluent.connect.jdbc.JdbcSourceConnector                 | RUNNING (1/1 tasks RUNNING)
 elasticsearch-sink | SINK   | io.confluent.connect.elasticsearch.ElasticsearchSinkConnector | WARNING (0/1 tasks RUNNING)
------------------------------------------------------------------------------------------------------------------------


CREATE STREAM FOOD_RAW (ID INTEGER, TITLES STRING) WITH (KAFKA_TOPIC='food_raw', KEY_FORMAT='KAFKA', PARTITIONS=2, REPLICAS=1, VALUE_FORMAT='AVRO');
SELECT * FROM food_raw
EMIT CHANGES;
CREATE STREAM food_sink
with (
    KAFKA_TOPIC = 'food_sink',
     KEY_FORMAT='KAFKA', PARTITIONS=2, REPLICAS=1, VALUE_FORMAT='AVRO') as SELECT titles
FROM food_raw 
EMIT CHANGES;


SELECT * FROM food_sink
EMIT CHANGES;


```
