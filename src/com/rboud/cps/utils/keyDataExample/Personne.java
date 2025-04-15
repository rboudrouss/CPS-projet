package com.rboud.cps.utils.keyDataExample;

import java.io.Serializable;
import java.util.Random;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Personne implements ContentDataI {
  private static int n = 0;

  public static final String NOM_ATTRIBUTE = "NOM";
  public static final String PRENOM_ATTRIBUTE = "PRENOM";
  public static final String AGE_ATTRIBUTE = "AGE";
  public static final String ID_ATTRIBUTE = "ID";
  public static final String[] ATTRIBUTES = { NOM_ATTRIBUTE, PRENOM_ATTRIBUTE, AGE_ATTRIBUTE, ID_ATTRIBUTE };

  private String nom;
  private String prenom;
  private int age;
  private int id;

  public Personne(String nom, String prenom, int age) {
    this.nom = nom;
    this.prenom = prenom;
    this.age = age;
    this.id = n;
    n += 1;
  }

  public String getNom() {
    return nom;
  }

  public String getPrenom() {
    return prenom;
  }

  public int getAge() {
    return age;
  }

  @Override
  public String toString() {
    return "Personne [nom=" + nom + ", prenom=" + prenom + ", age=" + age + ", id=" + id + "]";
  }

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

  public static Personne getRandomPersonne() {
    return new Personne(randomString(10), randomString(10), new Random().nextInt(100));
  }

  public static String randomString(int length) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    Random random = new Random();

    return random.ints(leftLimit, rightLimit + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  public Id getId() {
    return new Id(id);
  }

  public NameId getNameId() {
    return new NameId(nom + id);
  }

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
