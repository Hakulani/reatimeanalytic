## DADS6005 Realtime Analytics Quiz2
# Team member :
1. 6420422002 ธนากร วิธุรัติ

2. 6420422008 วรรณนภา ศรีเพ็ญ

3. 6420422011 จันทร์ทนีย์ พัฒนสุขกุล

4. 6420422017 วิศรุต วงศ์ซิ้ม

5. 6420422021 สุชาวลี จีระธัญญาสกุล
## Assignment :

 - Use Python to write the data source line-by-line from the CSV to SQL database (DB) (source part). The writing duration (d) must be random and between 0 < d < 2 seconds.
- Connect the SQL DB to a Kafka cluster via topic1.
- Clean the data in topic1 using ksqlDB and then submit it to topic2.
- Analyze the data in topic2 using ksqlDB and then submit it to topic3.
The source part (NoSQL DB) will incrementally consume the data from topic3
- Visualize the real-time data from topic3 using tools such as Plotly, etc..
- Conduct analytics based on your questions
- Set up and answer three questions 
easy, e.g., simple statistics
medium, e.g., join, windowed, etc.
hard, e.g., analytical insight

 
 

## Objective:

The objective of this project is to design and implement a data-streaming and real-time analytics system that can be used to analyze college students' food and cooking preferences data from Kaggle. The project involves data cleansing and analytics to be done in real-time.

## Datasets:
https://www.kaggle.com/datasets/borapajo/food-choices?select=food_coded.csv
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
0. download zip file and unzip
cmd in folder
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
   "topics"='food_analyze');
   
   ```   


 
  6. Data visualization from mongoDB with python 
 Using Dash plotly
 
 6.1 app version

 ``` 
 python app.py
Dash is running on http://127.0.0.1:8050/ 
``` 
จาก Histrogram เพศชายมีน้ำหนักมากกว่าเพศหญิง
![MicrosoftTeams-image (1)](https://user-images.githubusercontent.com/61573397/236524985-42c3e9c0-0ffc-4640-995e-9238164505a0.png)
 
 
 ``` 
python app2.py
Dash is running on http://127.0.0.1:8050/ 
``` 
6.2 jupyter notebook version

จาก Pivot table 
คนที่กิน comford food กินเพราะว่าช่วยบรรเทาการเครียด และมีความเบื่ออาหารและ เพศหญิงส่วนใหญ่ให้ความสำคัญเกี่ยวกับการนับแคลลอรี่อาหารต่อวัน 
![01](https://user-images.githubusercontent.com/61573397/236533746-3bb290d7-c1fc-41ae-abe5-20181a8848e4.jpg)
![02](https://user-images.githubusercontent.com/61573397/236533759-c9c25521-e603-40df-b4c0-60564c8b25c5.jpg)
วิธีการไดเอทแต่ละชั้นปี ทุก Grade Level ใช้วิธี ทานผักผลไม้และอาหารเพื่อสุขภาพ
แต่วิธีการที่รองลงมาแตกต่างกันดังนี้
Senior : more protein
Junior : current diet
Sophomore & Freshman : home cooked / organic
![03](https://user-images.githubusercontent.com/61573397/236533763-59dd38f5-b49a-4254-9f21-238d698f670b.jpg)
คนส่วนมากคิดว่าอาหารที่มีแคลลอรรี่มากที่สุดคือ waffle รองลงมาคือ tortilla ในทางตรงกันข้ามคนส่วนใหญ่กลับคิดว่า scone มีแคลลอรี่น้อยที่สุด
![04](https://user-images.githubusercontent.com/61573397/236533765-99d3fe32-bf12-451c-82bf-890a9028de7d.jpg)
น้ำหนักและGPA ไม่ได้มีความสัมพันธ์เชิงเส้นต่อกัน
![05](https://user-images.githubusercontent.com/61573397/236533772-8839bb5d-16ea-41e8-89fc-d754bf887090.jpg)


  
 
