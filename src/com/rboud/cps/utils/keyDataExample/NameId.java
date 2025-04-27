package com.rboud.cps.utils.keyDataExample;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * A content key implementation based on a string identifier.
 * Used as an example data for identifying content in the DHT using name-based
 * keys.
 */
public class NameId implements ContentKeyI {
  /** The string identifier for this key */
  private final String id;

  /**
   * Creates a new NameId with the given string identifier.
   * 
   * @param id the string identifier
   */
  public NameId(String id) {
    this.id = id;
  }

  /**
   * Gets the string identifier of this key.
   * 
   * @return the string identifier
   */
  public String getId() {
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
   * Two NameId objects are equal if they have the same string identifier.
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
    NameId other = (NameId) obj;
    return id.equals(other.id);
  }

  /**
   * Generates a hash code for this key based on its string identifier.
   * 
   * @return hash code value
   */
  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
