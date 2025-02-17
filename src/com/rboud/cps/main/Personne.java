package com.rboud.cps.main;

import java.io.Serializable;
import java.util.Random;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Personne implements ContentDataI {
  private static int n = 0;

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
    return "Personne [nom=" + nom + ", prenom=" + prenom + ", age=" + age + "]";
  }

  public Serializable getValue(String attributeName) {
    if (attributeName.equals("NOM")) {
      return nom;
    } else if (attributeName.equals("PRENOM")) {
      return prenom;
    } else if (attributeName.equals("AGE")) {
      return age;
    }
    System.out.println("WARNING personne#getvalue : " + attributeName + " not found");
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
    return new NameId(nom);
  }
}
