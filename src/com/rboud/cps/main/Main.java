package com.rboud.cps.main;

import java.util.ArrayList;
import java.util.List;

import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

public class Main {

  public static void main(String[] args) {
    List<DHTNode> nodes = new ArrayList<>();
    DHTNode node2 = new DHTNode(null);
    DHTNode node1 = new DHTNode(node2);

    try {
      node1.putSync(null, null, null);
      node1.putSync(null, new Id(0), new Personne("Dupont", "Jean", 25));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    DHTFacade facade = new DHTFacade(null);
    nodes.add(node1);
    nodes.add(node2);

    try {
      facade.put(null, null);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    Id id = new Id(0);
    System.out.println(id.hashCode());
  }
}
