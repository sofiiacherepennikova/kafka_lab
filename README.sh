#!/bin/bash

# Installing the dependencies
#install docker from here first: https://docs.docker.com/desktop/install/mac-install/
pip3 install kafka-python
pip3 install lz4
pip3 install streamlit
pip3 install pandas
pip3 install plotly
xcode-select --install
pip install watchdog

# The paths to useful kafka *.sh tools
export KAFKA_ROOT_DIR=`pwd`
cd ${KAFKA_ROOT_DIR}
export KAFKA_DIR=${KAFKA_ROOT_DIR}/kafka_2.13-3.7.0
export PATH=${KAFKA_DIR}/bin:${PATH}

#Launch the Kafka Cluster with 1 zookeeper and 3 brokers
docker-compose -f docker-compose-cluster.yml up -d

#Check that all the Kafka Cluster containeers are up and works
docker-compose -f docker-compose-cluster.yml ps

#Create the required topics
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw_data --replication-factor 3 --partitions 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic processed_data --replication-factor 3 --partitions 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic visual_data --replication-factor 3 --partitions 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ml_results --replication-factor 3 --partitions 3

#Check the info about the topic - replication/partitioning info
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw_data

#Check that all the topics have been created successfully - all 4 topics have to be in list:
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Run the main application which
#   1) Read the main .csv dataset from Spotify_Tracks_Dataset.csv input file and put into 'raw_data' topic
#   2) Processing - filter the data and add 'pop', 'acoustic', 'disco' and 'k-pop' genre related data into 'processed_data' topic
#   3) Visualize some data:
#       a) Top of music ganres based on total popularity of tracks
#       b) Top-25 songs
#       c) Get Top-3 music genres and show Top-5 songs for each one
#       d) Show 'Feature Distribution' histograms
#       e) Put the data for visualization into 'visual_data' kafka topic
python3 Kafka_lab.py


#Stop the Kafka Cluster on Docker
docker-compose -f docker-compose-cluster.yml stop
#Kill the Kafka Cluster on Docker, remove all the containers
docker-compose -f docker-compose-cluster.yml down

# To check all the available containers
docker container ls
# To remove some of them if you need
docker rm -f <container-name>

# Use in case of you want to consume from the same topic with the same consumer_group several times
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group myConsumerGroup --reset-offsets --to-earliest --topic my_topic -execute