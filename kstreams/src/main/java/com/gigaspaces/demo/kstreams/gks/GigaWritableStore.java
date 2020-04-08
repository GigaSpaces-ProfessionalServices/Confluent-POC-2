package com.gigaspaces.demo.kstreams.gks;

public interface GigaWritableStore<K,V> extends GigaReadableStore<K,V> {

  void write(K key, V value);

}
