-- From http://docs.confluent.io/current/ksql/docs/tutorials/basics-docker.html#create-a-stream-and-table

-- Create a stream update_stream from the Kafka topic pageviews, specifying the value_format of DELIMITED
CREATE STREAM wgs_update_stream (n varchar, t bigint, p varchar, v varchar) WITH (kafka_topic='wgs-updates', value_format='JSON', partitions=8);

-- Create a persistent query by using the CREATE STREAM keywords to precede the SELECT statement
CREATE STREAM update_stream AS SELECT * FROM wgs_update_stream;
