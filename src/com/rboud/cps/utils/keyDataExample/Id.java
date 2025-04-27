package com.rboud.cps.utils.keyDataExample;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * A content key implementation based on an integer identifier.
 * Used as example id for identifying content in the DHT using numeric keys.
 */
public class Id implements ContentKeyI {
  /** The integer identifier for this key */
  private final Integer id;

  /**
   * Creates a new Id with the given integer identifier.
   * 
   * @param id the integer identifier
   */
  public Id(Integer id) {
    this.id = id;
  }

  /**
   * Gets the integer identifier of this key.
   * 
   * @return the integer identifier
   */
  public Integer getId() {
    return id;
  }

  /**
   * Returns a string representation of the key.
   * 
   * @return string representation of the identifier
   */
  @Override
  public String toString() {
    return id.toString();
  }

  /**
   * Checks if this key equals another object.
   * Two Id objects are equal if they have the same integer identifier.
   *
   * @param obj object to compare with
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    Id other = (Id) obj;
    return id.equals(other.id);
  }

  /**
   * Generates a hash code for this key based on its integer identifier.
   * 
   * @return hash code value
   */
  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
