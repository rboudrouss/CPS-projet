package com.rboud.cps.main;

import java.io.Serializable;

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
}
