package com.rboud.cps.main;

import com.rboud.cps.core.DHTEndpoint;
import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

public class Main {

  public static void main(String[] args) {
    boolean DEBUG_ID_INT = false;

    DHTEndpoint endpoint = new DHTEndpoint();

    DHTNode node = new DHTNode();
    endpoint.initialiseServerSide(node);

    DHTFacade facade = new DHTFacade(endpoint);
    endpoint.initialiseClientSide(facade);

    try {

      for (int i = 0; i < 10; i++) {
        Personne temp = Personne.getRandomPersonne();
        facade.put(DEBUG_ID_INT ? temp.getId() : temp.getNameId(), temp);
        System.out.println("PUT:" + temp.getId());
      }

      Personne p = Personne.getRandomPersonne();
      facade.put(DEBUG_ID_INT ? p.getId() : p.getNameId(), p);

      node.printChain("DEBUG");

      Personne temp = (Personne) facade.get(DEBUG_ID_INT ? p.getId() : p.getNameId());
      System.out.println("\nFOUND:" + temp);
      System.out.println("Should be equal to: " + p + "\n");

      Integer out = facade.mapReduce(
          (_) -> true,
          (e) -> e.getValue("AGE"),
          (a, n) -> (a + (Integer) n),
          (a, n) -> (a + n), 0 // formatting hack
      );
      System.out.println("Somme age : " + out);

    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
}
