package com.rboud.cps.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTNode implements ContentAccessSyncI, MapReduceSyncI {
  private Set<String> seenMapURIs = new HashSet<String>();
  private Set<String> seenReduceURIs = new HashSet<String>();
  private Set<String> seenPrintURIs = new HashSet<String>();

  public static final int MAX_VALUE = 6; // Must be >= 2

  // key: computationURI, value: results extended from Array
  private Map<String, ArrayList<?>> mapResults = new HashMap<>();

  private int minHash;
  private int maxHash;

  private DHTNode next; // CANNOT BE NULL
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  public DHTNode(DHTNode next, int minHash, int maxHash) {
    this.minHash = minHash; // can be negative, included
    this.maxHash = maxHash;
    this.next = next;
  }

  public DHTNode(DHTNode next) {
    this(next, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public DHTNode() {
    this(null);
    this.next = this;
  }

  public static boolean isBetween(int value, int min, int max) {
    return value >= min && value < max;
  }

  private boolean isBetween(int value) {
    return isBetween(value, minHash, maxHash);
  }

  private void checkMerge() {
    if (localStorage.size() < MAX_VALUE / 3 && next.localStorage.size() < MAX_VALUE / 3) {
      merge();
    }
  }

  private void merge() {
    if (next == this) {
      return;
    }

    // merge data
    localStorage.putAll(next.localStorage);
    next.localStorage.clear();

    // merge hash
    this.maxHash = next.maxHash;
    this.next = next.next;
  }

  private void checkSplit() {
    if (localStorage.size() > MAX_VALUE) {
      splitNode();
    }
  }

  private void splitNode() {
    int minHashValue = this.maxHash, maxHashValue = this.minHash;

    // find min and max hash
    for (ContentKeyI key : localStorage.keySet()) {
      int hash = key.hashCode();
      minHashValue = Math.min(minHashValue, hash);
      maxHashValue = Math.max(maxHashValue, hash);
    }

    // spliting on the middle of the range of hash values
    // When working with integer keys, hash values tends to be close to each other,
    // that's why we can use this method instead of juste cutting in half the range
    int newMinHash = Math.floorDiv(maxHashValue + maxHashValue, 2);
    int newMaxHash = maxHash;

    this.maxHash = newMinHash - 1;
    this.next = new DHTNode(next, newMinHash, newMaxHash);

    // Move data
    Map<ContentKeyI, ContentDataI> newLocalStorage = new HashMap<>();
    localStorage.entrySet().removeIf(entry -> {
      if (entry.getKey().hashCode() > maxHash) {
        newLocalStorage.put(entry.getKey(), entry.getValue());
        return true;
      }
      return false;
    });

    next.localStorage.putAll(newLocalStorage);
  }

  public String toString() {
    return "DHTNode [minHash=" + minHash + ", maxHash=" + maxHash + ", nbElements=" + localStorage.size() + "]";
  }

  public void printChain(String URI) {
    if (seenPrintURIs.contains(URI)) {
      System.out.println("INFO PRINTCHAIN loop detected (or called before previous computation ends)");
      return;
    }
    seenPrintURIs.add(URI);
    System.out.println("Node: " + this);
    System.out.println("Data: " + localStorage);
    next.printChain(URI);
    seenPrintURIs.remove(URI);
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return next.getSync(URI, key);

    return localStorage.get(key);
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return next.putSync(URI, key, value);

    ContentDataI out = localStorage.put(key, value);
    checkSplit();
    return out;
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return next.removeSync(URI, key);

    ContentDataI out = localStorage.remove(key);
    checkMerge();
    return out;
  }

  @Override
  public void clearComputation(String URI) throws Exception {}

  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    seenMapURIs.remove(URI);
    seenReduceURIs.remove(URI);
    mapResults.remove(URI);
  }

  @Override
  public <R extends Serializable> void mapSync(String URI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    if (seenMapURIs.contains(URI)) {
      System.out.println("INFO MAPSYNC loop detected (or called before previous computation ends) with URI" + URI);
      return;
    }
    seenMapURIs.add(URI);

    ArrayList<R> results = localStorage.values().stream()
        .filter(selector::test)
        .map(processor::apply)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    mapResults.put(URI, results);
    this.next.mapSync(URI, selector, processor);
    seenMapURIs.remove(URI);
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator,
      A acc)
      throws Exception {
    if (seenReduceURIs.contains(URI)) {
      System.out
          .println("INFO REDUCESYNC loop detected (or called before previous computation ends) with URI" + URI);
      return acc;
    }
    seenReduceURIs.add(URI);

    // HACK Il faut vérifier si le cast est possible
    // @SuppressWarnings("unchecked")
    ArrayList<R> data = (ArrayList<R>) mapResults.get(URI);
    if (data == null) {
      throw new Exception("No data found for URI " + URI);
    }

    A currentResult = data.stream().reduce(acc, reductor::apply, combinator::apply);

    A nextResult = next.reduceSync(URI, reductor, combinator, acc);
    currentResult = combinator.apply(currentResult, nextResult);

    seenReduceURIs.remove(URI);
    return currentResult;
  }
}
