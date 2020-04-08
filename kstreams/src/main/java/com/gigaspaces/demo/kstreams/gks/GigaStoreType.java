package com.gigaspaces.demo.kstreams.gks;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class GigaStoreType<K,V> implements QueryableStoreType<GigaReadableStore<K,V>> {

  @Override
  public boolean accepts(StateStore stateStore) {
    return stateStore instanceof GigaStateStore;
  }

  @Override
  public GigaReadableStore<K, V> create(StateStoreProvider stateStoreProvider, String storeName) {
    return new GigaStateStoreWrapper<K,V>(stateStoreProvider, storeName, this);
  }
}
