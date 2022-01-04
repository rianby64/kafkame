#!/bin/bash

#podman pod create -p 9092:9092 -p 9093:9093 --name kafkapod

#podman run -d --rm --name zookeper_server --pod kafkapod -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper:latest

#podman run -d --name kafka_server1 --pod kafkapod -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeper_server:2181 -e KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9092 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://10.0.0.6:9092 bitnami/kafka:latest
#podman run -d --name kafka_server2 --pod kafkapod -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeper_server:2181 -e KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9093 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://10.0.0.6:9093 bitnami/kafka:latest

#podman exec -it kafka_server1 bash

#kafka-topics.sh --create --zookeeper zookeper_server:2181 --replication-factor 1 --partitions 1 --topic signals --config retention.ms=2000
#kafka-topics.sh --alter --zookeeper zookeper_server:2181 --topic signals --config retention.ms=2000

#kafka-topics.sh --create --zookeeper zookeper_server:2181 --replication-factor 1 --partitions 1 --topic orders --config retention.ms=120000
#kafka-topics.sh --alter --zookeeper zookeper_server:2181 --topic orders --config retention.ms=120000

#kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --config retention.ms=120000
