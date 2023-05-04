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

```
