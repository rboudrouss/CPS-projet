package com.rboud.cps.main;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class NameId implements ContentKeyI {
  private final String id;
  
  public NameId(String id) {
    this.id = id;
  }
  
  public String getId() {
    return id;
  }

  @Override
  public String toString(){
    return id.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    NameId other = (NameId) obj;
    return id.equals(other.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
