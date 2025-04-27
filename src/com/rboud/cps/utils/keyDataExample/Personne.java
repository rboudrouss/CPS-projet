package com.rboud.cps.utils.keyDataExample;

import java.io.Serializable;
import java.util.Random;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

/**
 * Represents a person entity that implements the ContentDataI interface.
 * Used as an example data type for DHT storage and MapReduce operations.
 */
public class Personne implements ContentDataI {
  /** Counter for generating unique IDs */
  private static int n = 0;

  /** Attribute name constant for last name */
  public static final String NOM_ATTRIBUTE = "NOM";
  /** Attribute name constant for first name */
  public static final String PRENOM_ATTRIBUTE = "PRENOM";
  /** Attribute name constant for age */
  public static final String AGE_ATTRIBUTE = "AGE";
  /** Attribute name constant for ID */
  public static final String ID_ATTRIBUTE = "ID";
  /** List of all available attributes */
  public static final String[] ATTRIBUTES = { NOM_ATTRIBUTE, PRENOM_ATTRIBUTE, AGE_ATTRIBUTE, ID_ATTRIBUTE };

  /** Person's last name */
  private String nom;
  /** Person's first name */
  private String prenom;
  /** Person's age */
  private int age;
  /** Person's unique identifier */
  private int id;

  /**
   * Creates a new Person with the given attributes.
   *
   * @param nom    Person's last name
   * @param prenom Person's first name
   * @param age    Person's age
   */
  public Personne(String nom, String prenom, int age) {
    this.nom = nom;
    this.prenom = prenom;
    this.age = age;
    this.id = n;
    n += 1;
  }

  /**
   * Gets the person's last name.
   * 
   * @return the last name
   */
  public String getNom() {
    return nom;
  }

  /**
   * Gets the person's first name.
   * 
   * @return the first name
   */
  public String getPrenom() {
    return prenom;
  }

  /**
   * Gets the person's age.
   * 
   * @return the age
   */
  public int getAge() {
    return age;
  }

  /**
   * Returns a string representation of the person.
   * 
   * @return string containing all person attributes
   */
  @Override
  public String toString() {
    return "Personne [nom=" + nom + ", prenom=" + prenom + ", age=" + age + ", id=" + id + "]";
  }

  /**
   * Gets the value of the specified attribute.
   *
   * @param attributeName name of the attribute to retrieve
   * @return the value of the attribute as a Serializable object
   */
  public Serializable getValue(String attributeName) {
    if (attributeName.equals(Personne.NOM_ATTRIBUTE)) {
      return nom;
    } else if (attributeName.equals(Personne.PRENOM_ATTRIBUTE)) {
      return prenom;
    } else if (attributeName.equals(Personne.AGE_ATTRIBUTE)) {
      return age;
    } else if (attributeName.equals(Personne.ID_ATTRIBUTE)) {
      return id;
    }
    return "Attribute not found";
  }

  /**
   * Creates a new Person instance with random attributes.
   * 
   * @return a randomly generated Person
   */
  public static Personne getRandomPersonne() {
    return new Personne(randomString(10), randomString(10), new Random().nextInt(100));
  }

  /**
   * Generates a random string of specified length.
   *
   * @param length the length of the string to generate
   * @return random string of lowercase letters
   */
  public static String randomString(int length) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    Random random = new Random();

    return random.ints(leftLimit, rightLimit + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  /**
   * Gets an Id object based on the person's numeric ID.
   * 
   * @return Id object
   */
  public Id getId() {
    return new Id(id);
  }

  /**
   * Gets a NameId object based on the person's name and ID.
   * 
   * @return NameId object
   */
  public NameId getNameId() {
    return new NameId(nom + id);
  }

  /**
   * Checks if this person is equal to another object.
   *
   * @param o object to compare with
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Personne personne = (Personne) o;
    return id == personne.id && nom.equals(personne.nom) && prenom.equals(personne.prenom) && age == personne.age;
  }
}
