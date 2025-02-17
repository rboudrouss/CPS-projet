package com.rboud.cps.main;

import com.rboud.cps.core.DHTEndpoint;
import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

public class Main {

  public static void main(String[] args) {
    
    DHTEndpoint endpoint = new DHTEndpoint();
    
    DHTNode node = new DHTNode();
    endpoint.initialiseServerSide(node);


    DHTFacade facade = new DHTFacade(endpoint);
    endpoint.initialiseClientSide(facade);

    try {
      Personne p4 = Personne.getRandomPersonne();
      facade.put(new Id(1), Personne.getRandomPersonne());
      facade.put(new Id(2), Personne.getRandomPersonne());
      facade.put(new Id(3), Personne.getRandomPersonne());
      facade.put(new Id(4), p4);
      facade.put(new Id(5), Personne.getRandomPersonne());
      facade.put(new Id(6), Personne.getRandomPersonne());
      facade.put(new Id(7), Personne.getRandomPersonne());
      facade.put(new Id(8), Personne.getRandomPersonne());
      facade.put(new Id(9), Personne.getRandomPersonne());
      facade.put(new Id(10), Personne.getRandomPersonne());

      node.printChain("DEBUG");

      Personne p = (Personne) facade.get(new Id(4));
      System.out.println("FOUND at id 4:" + p);
      System.out.println("Should be equal to p4: " + p4);

      Integer out = facade.mapReduce((_) -> true, (e) -> e.getValue("AGE"), (a, n) -> (a + (Integer) n),
          (a, n) -> (a + n), 0);
      System.out.println("somme age : " + out);

    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
}
