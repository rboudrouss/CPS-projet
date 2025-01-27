package com.rboud.cps.core;

import java.util.HashMap;
import java.util.Map;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class DHTNode implements ContentAccessSyncI {
  
  private DHTNode next;
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();
  
  public DHTNode() {

  }
  
  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    ContentDataI data = localStorage.get(key);
    if (data == null && next != null) {
      return next.getSync(URI, key);
    }
    return data;
  }
  
  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    ContentDataI old = localStorage.get(key);
    localStorage.put(key, value);
    return old;
  }
  
  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    ContentDataI data = localStorage.remove(key);

    if (data == null && next != null) 
      return next.removeSync(URI, key);
    
    return data;
  }
  
  @Override
  public void clearComputation(String URI) throws Exception {
    
  }
}
