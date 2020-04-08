# GKS (Gigaspaces KStreams state store)

# GKS is a fully searchable state store for Kafka streams based on Gigaspaces. 

00. Install InsightEdge. Update the license file under $GS_HOME/gs-license.txt Modify the bin/set-gs-env.sh as required.
01. Install Kafka. Set the KAFKA_HOME environment variable.

1. Run bin/start-gs.sh script to start Gigaspaces
2. Run bin/start-kafka.sh
3. Run bin/deploy-gs.sh
4. Run bin/kafka-write-messages.sh to place data in Kafka topic.
5. Run com.gigaspaces.demo.kstreams.App Java program.


The Kafka StateStore example takes Kafka stream input in the form of a key: followed by text.

The key is used to uniquely identify a document stored in Gigaspaces. You can also think of it as a title of a document. With that we store a map that contains the words in the text and the counts.

For example:
key:this is is a test

Creates the following data structure that gets stored to Gigaspaces.
docId: key, map{ this=1, is=2, a=1, test=1, " "=4}

#screenshot folder
contains screenshot of kafka input data & output data from grid
