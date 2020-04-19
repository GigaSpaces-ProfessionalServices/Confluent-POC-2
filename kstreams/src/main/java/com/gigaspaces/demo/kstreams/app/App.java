package com.gigaspaces.demo.kstreams.app;

import com.gigaspaces.demo.kstreams.processors.CountingProcessorSupplier;
import com.gigaspaces.demo.kstreams.gks.GigaStoreBuilder;
import java.util.Properties;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.notify.SimpleNotifyContainerConfigurer;
import org.openspaces.events.notify.SimpleNotifyEventListenerContainer;

public class App {
    private GigaSpace client;


  public static Properties configure() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }
  public static final String STORE_NAME = "wordcount-store";
  public static final String SOURCE_TOPIC = "source-topic";
  public static final String TARGET_TOPIC = "target-topic";

  public void init(){
      UrlSpaceConfigurer configurer = new UrlSpaceConfigurer("jini://*/*/" + App.STORE_NAME);
      client = new GigaSpaceConfigurer(configurer).gigaSpace();

  }

  public static void main(String[] args) throws Exception {

    System.out.println("Staring application");

    final App app = new App();
    app.init();

    app.start();

    System.out.println("Application is started");

  }



  public void start() {

    final Topology topology = new Topology();
    final GigaStoreBuilder<String, Long> gsStoreBuilder = new GigaStoreBuilder<>(
            App.STORE_NAME, String.class, Long.class);
    gsStoreBuilder.gigaspaces(client);

    topology.addSource("Source", SOURCE_TOPIC)
            .addProcessor("Process", new CountingProcessorSupplier(), "Source")
            .addStateStore(gsStoreBuilder, "Process")
            .addSink("Sink", TARGET_TOPIC, new StringSerializer(), new LongSerializer(), "Process");

    final KafkaStreams streams = new KafkaStreams(topology, configure());

    streams.cleanUp();
    streams.start();
    //start notify

    SQLQuery<SpaceDocument> template =
            new SQLQuery<SpaceDocument>(STORE_NAME, "value > 1");
    SpaceDocument[] results = client.readMultiple(template);

    System.out.println("Current snapshot of the state store data where word count id greater than 1");
    for (SpaceDocument result :results){
      System.out.println(result.getProperty("key")+" -> "+result.getProperty("value"));
    }

    SimpleNotifyEventListenerContainer notifyEventListenerContainer = new SimpleNotifyContainerConfigurer(client)
            .template(template)
            .eventListenerAnnotation(new Object() {
              @SpaceDataEvent
              public void eventListener(SpaceDocument currentSpaceDocument) {
                System.out.println("Updated word : "+currentSpaceDocument.getProperty("key")+", count : "+currentSpaceDocument.getProperty("value"));
              }
            })
            .notifyUpdate(true)
            .notifyWrite(true)
            .notifyContainer();

// start the listener
    notifyEventListenerContainer.start();
    System.out.println("Subscribe to updates for any word that satisfy the condition (greater than 1 occurrence)");
    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
