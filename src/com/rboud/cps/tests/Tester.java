package com.rboud.cps.tests;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

import com.rboud.cps.utils.keyDataExample.Id;
import com.rboud.cps.utils.keyDataExample.Personne;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;

public class Tester {
  private final int NB_RANDOM_VALUES = 10;
  private boolean ALLOW_RANDOM = true;
  private boolean STOP_ON_FAILURE = true;

  private LogFunction logFunction = message -> System.out.println(message);

  private DHTServicesI dht;

  public Tester(DHTServicesI dht, LogFunction logFunction) throws Exception {
    this.logFunction = message -> logFunction.log("[TESTER] " + message);
    this.dht = dht;
    if (ALLOW_RANDOM) {
      this.logFunction.log("Random values are allowed");
      this.logFunction.log("Please note that you can disable random values by calling disableRandomTests()");
    } else {
      this.logFunction.log("Random values are not allowed");
    }
  }

  public void disableRandomTests() {
    this.ALLOW_RANDOM = false;
  }

  /**
   * Wrapper for the tests, logs the label and catches exceptions
   * logs Failure and the exception message if the test fails
   * logs Success if the test passes
   * 
   * @param label the label of the test
   * @param test  the test to run
   * 
   */
  public void test(String label, TestFunction test) {
    this.logFunction.log(label);

    try {
      test.test();
      this.logFunction.log("SUCCESS");
    } catch (Throwable e) {
      this.logFunction.log("FAILURE");
      this.logFunction.log(e.getMessage() + "\n");
      if (this.STOP_ON_FAILURE) {
        this.logFunction.log("STOP_ON_FAILURE enabled, Stopping tests");
        throw new RuntimeException(e);
      }
      e.printStackTrace();
    }

  }

  /**
   * Force assert method to throw an error if the condition is false
   * Works even if the assert statement is disabled
   * 
   * @param condition the condition to check
   * @param message   the message to display if the condition is false
   * @throws Error if the condition is false
   */
  public void forceAssert(boolean condition, String message) throws Error {
    if (!condition) {
      throw new Error(message);
    }
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

    Random random = new Random();

    int[] testHashs = { -1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, };

    test("Test 1", () -> {
      ContentDataI data;
      for (int i : testHashs) {
        data = this.dht.get(new Id(i));
        forceAssert(data == null, "Data should be null");
      }
    });

    test("Test 2", () -> {
      ContentDataI data;
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        int hash = random.nextInt();
        data = this.dht.get(new Id(hash));
        forceAssert(data == null, "Data not be null");
      }
    });
  }

  public void getFindsPut() throws Exception {
    this.logFunction.log("Get should return the data that was put");

    int[] testHashs = { -1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, };
    Personne[] testData = new Personne[testHashs.length];

    test("Test 1", () -> {
      ContentDataI data;
      for (int i = 0; i < testHashs.length; i++) {
        testData[i] = Personne.getRandomPersonne();
        this.dht.put(new Id(testHashs[i]), testData[i]);

        // testing if exists right after put
        data = this.dht.get(new Id(testHashs[i]));
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(testData[i]), "Data should be equal to the one that was put");
      }
    });

    test("Test 2", () -> {
      ContentDataI data;
      // testing if data still exists after put
      for (int i = 0; i < testHashs.length; i++) {
        data = this.dht.get(new Id(testHashs[i]));
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(testData[i]), "Data should be equal to the one that was put");
      }
    });

    test("cleanup", () -> {
      // removing the data
      for (int i = 0; i < testHashs.length; i++) {
        dht.remove(new Id(testHashs[i]));
      }
    });

    // --------------------
    // Random values
    // --------------------
    if (!this.ALLOW_RANDOM) {
      return;
    }

    Personne[] randomData = new Personne[NB_RANDOM_VALUES];

    test("Test 3", () -> {
      ContentDataI data;
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        randomData[i] = Personne.getRandomPersonne();
        this.dht.put(randomData[i].getNameId(), randomData[i]);

        // testing if exists right after put
        data = this.dht.get(randomData[i].getNameId());
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(randomData[i]), "Data should be equal to the one that was put");
      }
    });

    test("Test 4", () -> {
      ContentDataI data;

      for (Personne p : randomData) {
        data = this.dht.get(p.getNameId());
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(p), "Data should be equal to the one that was put");
      }
    });

    test("cleanup", () -> {
      // removing the data
      for (Personne p : randomData) {
        dht.remove(p.getNameId());
      }
    });
  }

  public void putShouldNeverFail() throws Exception {
    if (!this.ALLOW_RANDOM) {
      return;
    }

    this.logFunction.log("Put should never fail");
    test("Test 1", () -> {
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        Personne p = Personne.getRandomPersonne();
        this.dht.put(p.getNameId(), p);
        this.dht.remove(p.getNameId());
      }
    });
  }

  public void getShouldFailWhenNotFound() throws Exception {
    this.logFunction.log("Get should return null when data is not found");
    Random random = new Random();

    int[] populatedHashs = { Integer.MIN_VALUE, 0, 6728163, 72819361, 1828391 };
    int[] testHashs = { -1, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481 };

    test("Populate DHT", () -> {
      // populating the DHT
      for (int i : populatedHashs) {
        this.dht.put(new Id(i), Personne.getRandomPersonne());
      }
    });

    test("Test 1", () -> {
      ContentDataI data;
      for (int i : testHashs) {
        data = this.dht.get(new Id(i));
        forceAssert(data == null, "Data should be null");
      }
    });

    // remvoving the data
    test("cleanup", () -> {
      for (int i : populatedHashs) {
        dht.remove(new Id(i));
      }
    });

    // ----------------------
    // Random values
    // ----------------------
    if (!this.ALLOW_RANDOM) {
      return;
    }

    Integer[] randomHashs = new Integer[NB_RANDOM_VALUES];

    // populating the DHT with random values
    test("Populate DHT", () -> {
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        randomHashs[i] = random.nextInt();
        this.dht.put(new Id(randomHashs[i]), Personne.getRandomPersonne());
      }
    });

    test("Test 2", () -> {
      ContentDataI data;
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        int n = random.nextInt();
        if (!Arrays.asList(randomHashs).contains(n)) {
          data = this.dht.get(new Id(n));
          forceAssert(data == null, "Data should be null");
        }
      }
    });

    test("cleanup", () -> {
      // removing the data
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        dht.remove(new Id(randomHashs[i]));
      }
    });
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
    Random random = new Random();

    int[] testHashs = { -1, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481 };
    int[] populatedHashs = { Integer.MIN_VALUE, 0, 6728163, 72819361, 1828391 };

    // populating the DHT
    test("Populate DHT", () -> {
      for (int i : populatedHashs) {
        this.dht.put(new Id(i), Personne.getRandomPersonne());
      }
    });

    test("Test 1", () -> {
      ContentDataI data;

      // testing if data is not found
      for (int i : testHashs) {
        data = this.dht.remove(new Id(i));
        forceAssert(data == null, "Data should be null");
      }
    });

    test("Test 2", () -> {
      ContentDataI data;

      // removing the data
      for (int i : populatedHashs) {
        data = this.dht.remove(new Id(i));
        forceAssert(data != null, "Data should not be null");
      }
    });

    // ----------------------
    // Random values
    // ----------------------
    if (!this.ALLOW_RANDOM) {
      return;
    }

    Integer[] randomHashs = new Integer[NB_RANDOM_VALUES];

    test("Populate DHT", () -> {
      // populating the DHT with random values
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        randomHashs[i] = random.nextInt();
        this.dht.put(new Id(randomHashs[i]), Personne.getRandomPersonne());
      }
    });

    test("Test 3", () -> {
      ContentDataI data;
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        int n = random.nextInt();
        if (!Arrays.asList(randomHashs).contains(n)) {
          data = this.dht.remove(new Id(n));
          forceAssert(data == null, "Data should be null");
        }
      }
    });

    test("Test 4", () -> {
      ContentDataI data;
      // removing the data
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        data = dht.remove(new Id(randomHashs[i]));
        forceAssert(data != null, "Data should not be null");
      }
    });
  }

  public void removeFindsAndCorrectlyRemoves() throws Exception {
    this.logFunction.log("Remove should find and correctly remove the data");

    int[] testHashs = { -1, 0, 1, Integer.MAX_VALUE, 39192849, 27181930, 91030481, Integer.MIN_VALUE };
    Personne[] testData = new Personne[testHashs.length];

    test("Populate DHT", () -> {
      for (int i = 0; i < testHashs.length; i++) {
        testData[i] = Personne.getRandomPersonne();
        this.dht.put(new Id(testHashs[i]), testData[i]);
      }
    });

    test("Test 1", () -> {
      ContentDataI data;

      // testing if data is found and removed
      for (int i = 0; i < testHashs.length; i++) {
        data = this.dht.remove(new Id(testHashs[i]));
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(testData[i]), "Data should be equal to the one that was put");
      }
    });

    test("Test 2", () -> {
      ContentDataI data;
      // testing if data is not found
      for (int i = 0; i < testHashs.length; i++) {
        data = this.dht.remove(new Id(testHashs[i]));
        forceAssert(data == null, "Data should be null");
      }
    });

    // ----------------------
    // Random values
    // ----------------------

    if (!this.ALLOW_RANDOM) {
      return;
    }

    Personne[] randomData = new Personne[NB_RANDOM_VALUES];

    test("Populate DHT", () -> {
      // populating the DHT with random values
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        randomData[i] = Personne.getRandomPersonne();
        this.dht.put(randomData[i].getNameId(), randomData[i]);
      }
    });

    test("Test 3", () -> {
      ContentDataI data;

      // testing if data is found and removed
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        data = this.dht.remove(randomData[i].getNameId());
        forceAssert(data != null, "Data should not be null");
        forceAssert(data.equals(randomData[i]), "Data should be equal to the one that was put");
      }
    });

    test("Test 4", () -> {
      ContentDataI data;

      // testing if data is not found
      for (int i = 0; i < NB_RANDOM_VALUES; i++) {
        data = this.dht.remove(randomData[i].getNameId());
        forceAssert(data == null, "Data should be null");
      }
    });
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

    test("Test 1", () -> {
      int result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          412 // formatter hack
      );
      this.logFunction.log("There should be " + ((result / 412)) + " empty nodes. Result was " + result);
      forceAssert((result % 412 == 0 && result >= 412), "Result should be equal to acc");
    });

    test("Test 2", () -> {
      String result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue("notfound"),
          (a, b) -> a + (String) b,
          (a, b) -> a,
          "ouais");
      forceAssert(result.equals("ouais"), "Result should be equal to acc");
    });

    test("Test 3", () -> {
      Personne expeted = new Personne("ouais", "ouais", 0);
      Personne result = this.dht.mapReduce(
          (_i) -> true,
          x -> x,
          (a, b) -> new Personne(a.getNom(), ((Personne) b).getPrenom(), a.getAge() + ((Personne) b).getAge()),
          (a, b) -> a,
          expeted);
      forceAssert(result.equals(expeted), "Result should be equal to acc");
    });
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

    test("Populate DHT", () -> {
      // populating the DHT
      for (Personne p : population) {
        this.dht.put(p.getNameId(), p);
      }
    });

    // sum of ages
    test("Test 1", () -> {
      int result = this.dht.mapReduce(
          (_i) -> true,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          0);
      this.logFunction.log("Result was " + result);
      forceAssert(result == 175, "Result is not correct, expected 175 but got " + result);
    });

    // sum of ages if age >= 40
    test("Test 2", () -> {
      int result = this.dht.mapReduce(
          (p) -> ((Personne) p).getAge() >= 40,
          x -> x.getValue(Personne.AGE_ATTRIBUTE),
          (a, b) -> a + (int) b,
          (a, b) -> a + b,
          0);
      forceAssert(result == 85, "Result is not correct, expected 85 but got " + result);
    });

    // Names of the members of the Doe family concatenated
    test("Test 3", () -> {
      String result = this.dht.mapReduce(
          (p) -> ((Personne) p).getNom().equals("Doe"),
          x -> x.getValue(Personne.PRENOM_ATTRIBUTE),
          (a, b) -> a + (String) b,
          (a, b) -> a + b,
          "");
      forceAssert(result.equals("JohnJane") || result.equals("JaneJohn"),
          "Result is not correct, expected JohnJane or JaneJohn but got " + result);
    });

    // Names of the members of the Doe family but as an array
    test("Test 4", () -> {
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
      forceAssert(result.length == 2, "Result is not correct, expected 2 elements but got " + result.length);
      forceAssert(Arrays.asList(result).contains("John"),
          "Result is not correct, expected a John but got " + Arrays.toString(result));
      forceAssert(Arrays.asList(result).contains("Jane"),
          "Result is not correct, expected a Jane but got " + Arrays.toString(result));
    });

    test("cleanup", () -> {
      // removing the data
      for (Personne p : population) {
        dht.remove(p.getNameId());
      }
    });
  }

  @FunctionalInterface
  public interface LogFunction {
    void log(String message);
  }

  @FunctionalInterface
  public interface TestFunction {
    void test() throws Throwable;
  }
}
