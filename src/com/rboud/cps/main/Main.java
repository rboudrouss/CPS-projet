package com.rboud.cps.main;

import com.rboud.cps.core.DHTFacade;

public class Main {

  public static void main(String[] args) {
    Personne personne = new Personne("Dupont", "Jean", 25);

    DHTFacade facade = new DHTFacade();

    try {
      facade.put(new Id(1), personne);
      facade.put(new Id(2), personne);
      facade.put(new Id(3), personne);
      facade.put(new Id(4), personne);
      facade.put(new Id(5), personne);
      facade.put(new Id(6), personne);
      facade.put(new Id(7), personne);
      facade.put(new Id(8), personne);
      facade.put(new Id(9), personne);

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
