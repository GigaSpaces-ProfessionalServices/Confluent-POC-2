package com.gigaspaces.demo.kstreams.gks;

import java.util.List;

public interface GigaReadableStore<K,V>  {

  V read(K key);

}
