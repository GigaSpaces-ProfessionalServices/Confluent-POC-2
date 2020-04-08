package com.gigaspaces.demo.kstreams.gks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gigaspaces.demo.kstreams.SerdesFactory;

import java.rmi.RemoteException;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;


/*
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
*/


public class GigaStateStore<K,V> implements StateStore, GigaWritableStore<K, V> {

    /* In Elasticsearch, an INDEX can be thought as logical area much like a db in a dbms or space in Gigaspaces.
     */
    private Class<K> Obj1;
    private Class<V> Obj2;

    public static final String INDEX = "words";
    public static final String TYPE_DESCRIPTOR_NAME = INDEX;

    public static String STORE_NAME = "GigaStateStore";

    private final String hostAddr;

    private final ObjectMapper mapper = new ObjectMapper();
    private GigaChangeLogger<K,V> changeLogger = null;
    private GigaSpace client;
    private ProcessorContext context;
    private long updateTimestamp;
    //private Document value;
    private String key;
    //private Serde<Document> docSerdes;

    public GigaStateStore(String hostAddr,Class<K> Obj1,Class<V> Obj2) {
        this.hostAddr = hostAddr;
        this.Obj1 = Obj1;
        this.Obj2 = Obj2;
    }

    @Override // GigaReadableStore
    public V read(K key) {
        if (key == null) {
            return null;
        }
        SpaceDocument spaceDocument = client.readById(new IdQuery<SpaceDocument>(TYPE_DESCRIPTOR_NAME, key));
        if( spaceDocument != null ) {
            V value = spaceDocument.getProperty("value");
            return value;
        }
        else { // no document found
            return null;
        }
    }

    @Override // GigaWritableStore
    public void write(K key, V value) {

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("key", key);
        properties.put("value", value);

        SpaceDocument spaceDocument = new SpaceDocument(TYPE_DESCRIPTOR_NAME, properties);

        client.write(spaceDocument);

        this.updateTimestamp = System.currentTimeMillis();
    }

    @Override // StateStore
    public String name() {
        return STORE_NAME;
    }

    @Override // StateStore //aaa
    public void init(ProcessorContext processorContext, StateStore stateStore) {

        context = processorContext;
        UrlSpaceConfigurer configurer = new UrlSpaceConfigurer("jini://*/*/" + INDEX);
        client = new GigaSpaceConfigurer(configurer).gigaSpace();

      //  HashMap<String,Long> map = new HashMap<>();
        // register type
        SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(TYPE_DESCRIPTOR_NAME)
                .idProperty("key", false, SpaceIndexType.EQUAL)
                .addFixedProperty("key", Obj1)
                .addFixedProperty("value", Obj2)
                .addPropertyIndex("content", SpaceIndexType.EQUAL)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);

//        docSerdes = SerdesFactory.from(Document.class);

  /*      StateSerdes<K,V> serdes = new StateSerdes(
                name(),
                Serdes.String(),
                docSerdes);*/

//        changeLogger = new GigaChangeLogger<K,V>(name(), context, serdes);

        context.register(this, (key, value) -> {

        });
    }

    @Override //StateStore
    public void flush() {
    /*
      Definition of a flush, for which there is no GigaSpace equivalent
       - Flush essentially means that all the documents in the in-memory buffer are written to new Lucene segments,
       - These, along with all existing in-memory segments, are committed to the disk, which clears the translog.
       - This commit is essentially a Lucene commit.

    org.elasticsearch.action.admin.indices.flush.FlushRequest FlushRequest request = new FlushRequest(INDEX);

    try {
      client.indices().flush(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
    }
        changeLogger.logChange(key, value, updateTimestamp);
     */
    }

    @Override // StateStore
    public void close() {
    /*
      We don't have a GigaSpace.close equivalent, unless we use internal API
    try {
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    */
    }

    @Override //StateStore
    public boolean persistent() {
        return true;
    }

    @Override //StateStore
    public boolean isOpen() {
    /*
    try {
      return client.ping(RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

     */
        try {
            client.getSpace().ping();
            return true;
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return false;
    }

}
