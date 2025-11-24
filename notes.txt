#cold start:

curl -X DELETE http://localhost:8083/connectors/postgres-connector
curl -X POST -H "Content-Type: application/json" \
  --data @register_postgres.json \
  http://localhost:8083/connectors

curl http://localhost:8083/connectors/postgres-connector

# odświeżenie konektora = DELETE + POST

#uruchomienie terminalu kafka w dockerze:
docker exec -it debezium bash

#kafka podejrzenie zwykłe:

kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic wpdb.public.data1 \
  --from-beginning

#kafka podejrzenie extended:

kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic wpdb.public.data1 \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

#kafka czyszczenie logów:

kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic wpdb.public.data1

#wejście do db przez konsolę

docker exec -it pg_task1 psql -U pguser -d business_db

#i można SELECT * FROM public.data1 itp.