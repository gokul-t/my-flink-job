#### Build and Run
```shell
podman-compose up -d;
```

submit a job
```
docker exec -it jobmanager sh -c "./bin/flink run /job.jar"
```; 

#### create kafka topics
```shell
# create input & output topics
docker exec -it kafka /bin/bash

# inside container (bitnami images include kafka binaries at /opt/bitnami/kafka/bin)
# or use kafka-topics.sh path depending on image layout:
kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```

#### test end-to-end
produce message
```shell
# open a shell in kafka container (or use kafka-console-producer from host if you have kafka tools)
docker exec -it kafka bash
kafka-console-producer.sh --topic input-topic --bootstrap-server localhost:9092
# type lines like:
hello flink
foo bar
```
consume message
```shell
# open a shell in kafka container (or use kafka-console-producer from host if you have kafka tools)
docker exec -it kafka bash
kafka-console-consumer.sh --topic output-topic --bootstrap-server localhost:9092 --from-beginning
```

#### stop
```shell
podman-compose down -v;
```