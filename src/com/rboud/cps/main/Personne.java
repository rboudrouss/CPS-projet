package com.rboud.cps.main;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

// ContentKey is interger

public class Personne implements ContentDataI {
  private String nom;
  private String prenom;
  private int age;

  public Personne(String nom, String prenom, int age) {
    this.nom = nom;
    this.prenom = prenom;
    this.age = age;
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

  public String getValue(String attributeName) {
    if (attributeName.equals("nom")) {
      return nom;
    } else if (attributeName.equals("prenom")) {
      return prenom;
    } else if (attributeName.equals("age")) {
      return Integer.toString(age);
    }
    return "Attribute not found";
  }
}
