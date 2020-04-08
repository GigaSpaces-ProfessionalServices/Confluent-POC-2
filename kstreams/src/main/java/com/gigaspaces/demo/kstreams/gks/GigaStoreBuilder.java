package com.gigaspaces.demo.kstreams.gks;

import java.util.Map;
import org.apache.kafka.streams.state.StoreBuilder;

public class GigaStoreBuilder<K,V> implements StoreBuilder<GigaStateStore> {

  private final String hostAddr;
  private Map<String, String> config;
  Class<K> Obj1;
  Class<V> Obj2;
  public GigaStoreBuilder(Class<K> Obj1,Class<V> Obj2) {
    this("http://localhost:9200",Obj1, Obj2);
  }

  public GigaStoreBuilder(String hostAddr,Class<K> Obj1,Class<V> Obj2) {
    this.hostAddr = hostAddr;
    this.Obj1 = Obj1;
    this.Obj2 = Obj2;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withCachingEnabled() {
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withCachingDisabled() {
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore> withLoggingEnabled(Map<String, String> config) {
    this.config = config;
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withLoggingDisabled() {
    return this;
  }

  @Override
  public GigaStateStore build() {
    return new GigaStateStore(hostAddr,Obj1,Obj2);
  }

  @Override
  public Map<String, String> logConfig() {
    return config;
  }

  @Override
  public boolean loggingEnabled() {
    return false;
  }

  @Override
  public String name() {
    return GigaStateStore.STORE_NAME;
  }
}
