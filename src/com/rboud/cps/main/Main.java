package com.rboud.cps.main;

import com.rboud.cps.core.DHTFacade;

public class Main {

  public static void main(String[] args) {
    DHTFacade facade = new DHTFacade();

    try {
      facade.put(new Id(1), Personne.getRandomPersonne());
      facade.put(new Id(2), Personne.getRandomPersonne());
      facade.put(new Id(3), Personne.getRandomPersonne());
      facade.put(new Id(4), Personne.getRandomPersonne());
      facade.put(new Id(5), Personne.getRandomPersonne());
      facade.put(new Id(6), Personne.getRandomPersonne());
      facade.put(new Id(7), Personne.getRandomPersonne());
      facade.put(new Id(8), Personne.getRandomPersonne());
      facade.put(new Id(9), Personne.getRandomPersonne());
      facade.put(new Id(10), Personne.getRandomPersonne());

      Personne p = (Personne) facade.get(new Id(1));
      System.out.println(p);

      facade.printChainNode();

      Integer out = facade.mapReduce((_) -> true, (e) -> e.getValue("AGE"), (a, n) -> (a + (Integer) n),
          (a, n) -> (a + n), 0);
      System.out.println("somme age : " + out);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
}
