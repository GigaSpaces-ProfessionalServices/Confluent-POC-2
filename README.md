# GKS (Gigaspaces KStreams state store)

## GKS is a fully searchable state store for Kafka streams based on Gigaspaces. 

##Word count KStream State Store example
00. Install InsightEdge. Update the license file under $GS_HOME/gs-license.txt Modify the bin/set-gs-env.sh as required.
1. Install Kafka. Set the KAFKA_HOME environment variable.
2. Run ```bin/start_gs.sh``` to sart a gigaspces server
3. Run ```bin/init_gs_cache.sh``` to deploy a two partitions wordcount-store space 
4. Start Kafka server
5. Run ```bin/create_topics.sh```
6. Run ```bin/start_app.sh``` to run the word count state store app. In addition, this java app will also run a continues query using the Gigaspaces client and print the progress.
7. run ```bin/produce_test_data.sh``` to push some test data into kafka 

## Seperate demo (can use the same wordcount-store space) show some query capabilities of Gigaspaces 
This demo populate the space with a few test records based on predefined Pojo model (com.gigaspaces.demo.kstreams.model). It then show you how to run text search using lucene indexes as well as running server side code to join data from different types (in the case, Person and Organization based on Org Id foreign key)

8. run ```bin/start_data_grid_test.sh```