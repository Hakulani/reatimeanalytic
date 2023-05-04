CREATE SOURCE CONNECTOR `postgres-source` WITH(
    "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
    "connection.url"='jdbc:postgresql://postgres:5432/root?user=root&password=secret',
    "topic.prefix"='food',
    "mode"='bulk',
    "table.whitelist"='food');


CREATE SINK CONNECTOR `elasticsearch-sink` WITH(
    "connector.class"='io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    "connection.url"='http://elasticsearch:9200',
    "connection.username"='',
    "connection.password"='',
    "batch.size"='1',
    "write.method"='insert',
    "topics"='food',
    "type.name"='changes');
