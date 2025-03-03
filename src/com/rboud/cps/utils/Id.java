package com.rboud.cps.utils;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class Id implements ContentKeyI {
  private final Integer id;

  public Id(Integer id) {
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  @Override
  public String toString() {
    return id.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    Id other = (Id) obj;
    return id.equals(other.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

}
