
conda activate pythonspark


spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.6,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3  --executor-memory 10G  --driver-memory 10G /Crypto/Cold_Data/Spark/Spark.py
