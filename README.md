# spark-kafka-stucrured-streaming
Sample application for spark kafka stuctured streaming

# Setting up kafka locally

1. Download kafka version kafka_2.12-3.2.1
2. Use below commands to start local cluster with console producer and consumer

cd KAFKA_DOWNLOAD_PATH\kafka_2.12-3.2.1

bin\windows\zookeeper-server-start.bat KAFKA_DOWNLOAD_PATH\kafka_2.12-3.2.1\config\zookeeper.properties

bin\windows\kafka-server-start.bat KAFKA_DOWNLOAD_PATH\kafka_2.12-3.2.1\config\server.properties


bin\windows\kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test


bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning



bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic test

bin\windows\kafka-topics.bat --delete --bootstrap-server localhost:9092 --topic test

bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic test

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

# To run spark streaming locally, we always face problems with the winutils utility and its version.

1. Download the winutils version same as spark version you are using.
2. Set env varibale for winutils till path e.g. C:\winutils
3. Set HADOOP_HOME Env variable with this path: C:\winutils
4. Create bin folder under the C:\winutils and copy 2 jars from downloded hadoop distribution to bin.
winutils
hadoop.dll

