package com.gigaspaces.demo.kstreams.gks;

import com.fasterxml.jackson.databind.ObjectMapper;

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
    private Class<K> Obj1;
    private Class<V> Obj2;

    public static final String INDEX = "words";
    public static final String TYPE_DESCRIPTOR_NAME = INDEX;

    public static String STORE_NAME = "GigaStateStore";


    private GigaSpace client;
    private ProcessorContext context;

    public GigaStateStore(String hostAddr,Class<K> Obj1,Class<V> Obj2) {
        this.Obj1 = Obj1;
        this.Obj2 = Obj2;
    }

    @Override // GigaReadableStore
    public V read(K key) {
        if (key == null) {
            return null;
        }
        SpaceDocument spaceDocument = client.readById(new IdQuery<>(TYPE_DESCRIPTOR_NAME, key));
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

        // register type
        SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(TYPE_DESCRIPTOR_NAME)
                .idProperty("key", false, SpaceIndexType.EQUAL)
                .addFixedProperty("key", Obj1)
                .addFixedProperty("value", Obj2)
                .addPropertyIndex("content", SpaceIndexType.EQUAL)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);


        context.register(this, (key, value) -> {

        });
    }

    @Override //StateStore
    public void flush() {
    /*
      Definition of a flush, for which there is no GigaSpace equivalent
     */
    }

    @Override // StateStore
    public void close() {
    /*
      We don't have a GigaSpace.close equivalent, unless we use internal API
    */
    }

    @Override //StateStore
    public boolean persistent() {
        return true;
    }

    @Override //StateStore
    public boolean isOpen() {
            try {
            client.getSpace().ping();
            return true;
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return false;
    }

}
