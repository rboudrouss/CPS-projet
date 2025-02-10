package com.rboud.cps.main;

import java.util.ArrayList;
import java.util.List;

import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

public class Main {

  public static void main(String[] args) {
    List<DHTNode> nodes = new ArrayList<>();
    DHTNode node2 = new DHTNode();
    DHTNode node1 = new DHTNode(node2);

    Personne personne = new Personne("Dupont", "Jean", 25);

    try {
      node1.putSync("", new Id(0), personne);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    DHTFacade facade = new DHTFacade();
    nodes.add(node1);
    nodes.add(node2);

    try {
      facade.put(new Id(5), personne);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
