package com.rboud.cps.tests;

import java.util.Arrays;
import java.util.Random;

import com.rboud.cps.utils.keyDataExample.Id;
import com.rboud.cps.utils.keyDataExample.Personne;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;

public class Tester {
  private final int NB_RANDOM_VALUES = 10;
  private boolean allowRandomValues = true;

  private LogFunction logFunction = message -> System.out.println(message);

  private DHTServicesI dht;

  public Tester(DHTServicesI dht, LogFunction logFunction) throws Exception {
    boolean assertEnabled = false;
    try {
      assert false;
    } catch (AssertionError e) {
      assertEnabled = true;
    }

    if (!assertEnabled) {
      logFunction.log("[TESTER] <!> ERROR: Assertions are not enabled, quitting...");
      throw new RuntimeException("Assertions are not enabled. Please enable assertions to run the tests.");
    }

    this.logFunction = message -> logFunction.log("[TESTER] " + message);
    this.dht = dht;
    if (allowRandomValues) {
      this.logFunction.log("Random values are allowed");
      this.logFunction.log("Please note that you can disable random values by calling disableRandomTests()");
    } else {
      this.logFunction.log("Random values are not allowed");
    }
  }

  public void disableRandomTests() {
    this.allowRandomValues = false;
  }

  // ------------------------------------------------------------------------
  // General
  // ------------------------------------------------------------------------

  public void allTesting() throws Exception {
    this.logFunction.log("Testing all methods");
    this.getAndPutTesting();
    this.removeTesting();
    this.mapReduceTesting();
    this.logFunction.log("All tests passed");
  }

  // ------------------------------------------------------------------------
  // PUT & GET
  // ------------------------------------------------------------------------

  public void getAndPutTesting() throws Exception {
    this.logFunction.log("");
    this.logFunction.log("Testing get & put methods");
    this.logFunction.log("Please note that we rely on the remove method to remove the data from the DHT");
    this.getShouldFailWhenEmpty();
    this.getFindsPut();
    this.getShouldFailWhenNotFound();
    this.putShouldNeverFail();
  }

  public void getShouldFailWhenEmpty() throws Exception {
    this.logFunction.log("Get should return null when data is empty");

    ContentDataI data;
    Random random = new Random();

    int[] testHashs = { -1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, };

    this.logFunction.log("Test 1");
    for (int i : testHashs) {
      data = this.dht.get(new Id(i));
      assert data == null : "Data should be null";
    }

    this.logFunction.log("Test 2");
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      int hash = random.nextInt();
      data = this.dht.get(new Id(hash));
      assert data == null : "Data not be null";
    }
  }

  public void getFindsPut() throws Exception {
    this.logFunction.log("Get should return the data that was put");

    ContentDataI data;

    int[] testHashs = { -1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, };
    Personne[] testData = new Personne[testHashs.length];

    this.logFunction.log("Test 1");
    for (int i = 0; i < testHashs.length; i++) {
      testData[i] = Personne.getRandomPersonne();
      this.dht.put(new Id(testHashs[i]), testData[i]);

      // testing if exists right after put
      data = this.dht.get(new Id(testHashs[i]));
      assert data != null : "Data should not be null";
      assert data.equals(testData[i]) : "Data should be equal to the one that was put";
    }

    this.logFunction.log("Test 2");
    // Testing if data still exists after put
    for (int i = 0; i < testHashs.length; i++) {
      data = this.dht.get(new Id(testHashs[i]));
      assert data != null : "Data should not be null";
      assert data.equals(testData[i]) : "Data should be equal to the one that was put";
    }

    // removing the data
    for (int i = 0; i < testHashs.length; i++) {
      dht.remove(new Id(testHashs[i]));
    }

    // --------------------
    // Random values
    // --------------------
    if (!this.allowRandomValues) {
      return;
    }

    this.logFunction.log("Test 3");
    Personne[] randomData = new Personne[NB_RANDOM_VALUES];
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      randomData[i] = Personne.getRandomPersonne();
      this.dht.put(randomData[i].getNameId(), randomData[i]);

      // testing if exists right after put
      data = this.dht.get(randomData[i].getNameId());
      assert data != null : "Data should not be null";
      assert data.equals(randomData[i]) : "Data should be equal to the one that was put";
    }

    this.logFunction.log("Test 4");
    for (Personne p : randomData) {
      data = this.dht.get(p.getNameId());
      assert data != null : "Data should not be null";
      assert data.equals(p) : "Data should be equal to the one that was put";
    }

    // removing the data
    for (Personne p : randomData) {
      dht.remove(p.getNameId());
    }
  }

  public void putShouldNeverFail() throws Exception {
    if (!this.allowRandomValues) {
      return;
    }

    this.logFunction.log("Put should never fail");
    this.logFunction.log("Test 1");
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      Personne p = Personne.getRandomPersonne();
      this.dht.put(p.getNameId(), p);
      this.dht.remove(p.getNameId());
    }
  }

  public void getShouldFailWhenNotFound() throws Exception {
    this.logFunction.log("Get should return null when data is not found");
    ContentDataI data;
    Random random = new Random();

    int[] populatedHashs = { Integer.MIN_VALUE, 0, 6728163, 72819361, 1828391 };
    int[] testHashs = { -1, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481 };

    for (int i : populatedHashs) {
      this.dht.put(new Id(i), Personne.getRandomPersonne());
    }

    this.logFunction.log("Test 1");
    for (int i : testHashs) {
      data = this.dht.get(new Id(i));
      assert data == null : "Data should be null";
    }

    // remvoving the data
    for (int i : populatedHashs) {
      dht.remove(new Id(i));
    }

    // ----------------------
    // Random values
    // ----------------------
    if (!this.allowRandomValues) {
      return;
    }

    Integer[] randomHashs = new Integer[NB_RANDOM_VALUES];

    // populating the DHT with random values
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      randomHashs[i] = random.nextInt();
      data = this.dht.put(new Id(randomHashs[i]), Personne.getRandomPersonne());
    }

    this.logFunction.log("Test 2");
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      int n = random.nextInt();
      if (!Arrays.asList(randomHashs).contains(n)) {
        data = this.dht.get(new Id(n));
        assert data == null : "Data should be null";
      }
    }

    // removing the data
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      dht.remove(new Id(randomHashs[i]));
    }
  }

  // ------------------------------------------------------------------------
  // Remove
  // ------------------------------------------------------------------------

  public void removeTesting() throws Exception {
    this.logFunction.log("");
    this.logFunction.log("Testing remove method");
    this.removeShouldFailWhenNotFound();
    this.removeFindsAndCorrectlyRemoves();
  }

  public void removeShouldFailWhenNotFound() throws Exception {
    this.logFunction.log("Remove should fail when data is not found");
    ContentDataI data;
    Random random = new Random();

    int[] testHashs = { -1, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481 };
    int[] populatedHashs = { Integer.MIN_VALUE, 0, 6728163, 72819361, 1828391 };

    // populating the DHT
    for (int i : populatedHashs) {
      this.dht.put(new Id(i), Personne.getRandomPersonne());
    }

    this.logFunction.log("Test 1");
    // testing if data is not found
    for (int i : testHashs) {
      data = this.dht.remove(new Id(i));
      assert data == null : "Data should be null";
    }

    this.logFunction.log("Test 2");
    // removing the data
    for (int i : populatedHashs) {
      data = this.dht.remove(new Id(i));
      assert data != null : "Data should not be null";
    }

    // ----------------------
    // Random values
    // ----------------------
    if (!this.allowRandomValues) {
      return;
    }

    Integer[] randomHashs = new Integer[NB_RANDOM_VALUES];

    // populating the DHT with random values
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      randomHashs[i] = random.nextInt();
      data = this.dht.put(new Id(randomHashs[i]), Personne.getRandomPersonne());
    }

    this.logFunction.log("Test 3");
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      int n = random.nextInt();
      if (!Arrays.asList(randomHashs).contains(n)) {
        data = this.dht.remove(new Id(n));
        assert data == null : "Data should be null";
      }
    }

    this.logFunction.log("Test 4");
    // removing the data
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      data = dht.remove(new Id(randomHashs[i]));
      assert data != null : "Data should not be null";
    }
  }

  public void removeFindsAndCorrectlyRemoves() throws Exception {
    this.logFunction.log("Remove should find and correctly remove the data");
    ContentDataI data;

    int[] testHashs = { -1, 0, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481, Integer.MIN_VALUE };
    Personne[] testData = new Personne[testHashs.length];

    for (int i = 0; i < testHashs.length; i++) {
      testData[i] = Personne.getRandomPersonne();
      this.dht.put(new Id(testHashs[i]), testData[i]);
    }

    this.logFunction.log("Test 1");
    // testing if data is found and removed
    for (int i = 0; i < testHashs.length; i++) {
      data = this.dht.remove(new Id(testHashs[i]));
      assert data != null : "Data should not be null";
      assert data.equals(testData[i]) : "Data should be equal to the one that was put";
    }

    this.logFunction.log("Test 2");
    // testing if data is not found
    for (int i = 0; i < testHashs.length; i++) {
      data = this.dht.remove(new Id(testHashs[i]));
      assert data == null : "Data should be null";
    }

    // ----------------------
    // Random values
    // ----------------------

    if (!this.allowRandomValues) {
      return;
    }

    Personne[] randomData = new Personne[NB_RANDOM_VALUES];

    // populating the DHT with random values
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      randomData[i] = Personne.getRandomPersonne();
      data = this.dht.put(randomData[i].getNameId(), randomData[i]);
    }

    this.logFunction.log("Test 3");
    // testing if data is found and removed
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      data = this.dht.remove(randomData[i].getNameId());
      assert data != null : "Data should not be null";
      assert data.equals(randomData[i]) : "Data should be equal to the one that was put";
    }

    this.logFunction.log("Test 4");
    // testing if data is not found
    for (int i = 0; i < NB_RANDOM_VALUES; i++) {
      data = this.dht.remove(randomData[i].getNameId());
      assert data == null : "Data should be null";
    }
  }

  // ------------------------------------------------------------------------
  // MAP REDUCE
  // ------------------------------------------------------------------------

  public void mapReduceTesting() throws Exception {
    this.logFunction.log("");
    this.logFunction.log("Testing map reduce methods");
    this.mapReduceReturnsAccWhenEmpty();
    this.mapReduceReturnsCorrectValue();
  }

  public void mapReduceReturnsAccWhenEmpty() throws Exception {
    this.logFunction.log("Map reduce should return acc when data is empty");

    this.logFunction.log("Test 1");
    {
      int result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          412 // formatter hack
      );
      this.logFunction.log("There should be " + ((result / 412) - 1) + " empty nodes. Result was " + result);
      assert (result % 412 == 0) : "Result should be equal to acc";
    }

    this.logFunction.log("Test 2");
    {
      String result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue("notfound"),
          (a, b) -> a + (String) b,
          (a, b) -> a,
          "ouais");
      assert result.equals("ouais") : "Result should be equal to acc";
    }

    this.logFunction.log("Test 3");
    {
      Personne expeted = new Personne("ouais", "ouais", 0);
      Personne result = this.dht.mapReduce(
          (_i) -> true,
          x -> x,
          (a, b) -> new Personne(a.getNom(), ((Personne) b).getPrenom(), a.getAge() + ((Personne) b).getAge()),
          (a, b) -> a,
          expeted);
      assert result.equals(expeted) : "Result should be equal to acc";
    }
  }

  public void mapReduceReturnsCorrectValue() throws Exception {
    this.logFunction.log("Map reduce should return the correct value");

    Personne[] population = new Personne[] {
        new Personne("Doe", "John", 25),
        new Personne("Doe", "Jane", 30),
        new Personne("Alice", "Smith", 35),
        new Personne("Bob", "Johnson", 40),
        new Personne("Charlie", "Brown", 45),
    };

    // populating the DHT
    for (Personne p : population) {
      this.dht.put(p.getNameId(), p);
    }

    this.logFunction.log("Test 1");
    // sum of ages
    {
      int result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          0);
      assert result == 175 : "Result is not correct";
    }

    this.logFunction.log("Test 2");
    // sum of ages if age >= 40
    {
      int result = this.dht.mapReduce(
          (p) -> ((Personne) p).getAge() >= 40,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          0);
      assert result == 85 : "Result is not correct";
    }

    this.logFunction.log("Test 3");
    // Names of the members of the Doe family concatenated
    {
      String result = this.dht.mapReduce(
          (p) -> ((Personne) p).getNom().equals("Doe"),
          x -> x.getValue(Personne.PRENOM_ATTRIBUTE),
          (a, b) -> a + (String) b,
          (a, b) -> a + b,
          "");
      assert result.equals("JohnJane") || result.equals("JaneJohn") : "Result is not correct";
    }

    this.logFunction.log("Test 4");
    // Names of the members of the Doe family but as an array
    {
      String[] result = this.dht.mapReduce(
          (p) -> ((Personne) p).getNom().equals("Doe"),
          x -> x.getValue(Personne.PRENOM_ATTRIBUTE),
          (a, b) -> {
            String[] newArray = new String[a.length + 1];
            System.arraycopy(a, 0, newArray, 0, a.length);
            newArray[a.length] = (String) b;
            return newArray;
          },
          (a, b) -> {
            String[] newArray = new String[a.length + b.length];
            System.arraycopy(a, 0, newArray, 0, a.length);
            System.arraycopy(b, 0, newArray, a.length, b.length);
            return newArray;
          },
          new String[0]);
      assert result.length == 2 : "Result is not correct";
      assert Arrays.asList(result).contains("John") : "Result is not correct";
      assert Arrays.asList(result).contains("Jane") : "Result is not correct";
    }

    // remove data
    for (Personne p : population) {
      this.dht.remove(p.getNameId());
    }
  }

  @FunctionalInterface
  public interface LogFunction {
    void log(String message);
  }
}
