package com.rboud.cps.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rboud.cps.connections.DHTContentAccessInboundPort;
import com.rboud.cps.connections.DHTMapReduceInboundPort;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class DHTNode extends AbstractComponent implements ContentAccessSyncCI, MapReduceSyncCI {
  private Set<String> seenURIs = new HashSet<String>();
  private Set<String> seenPrintURIs = new HashSet<String>();

  // max element in node, Must be >= 2
  public static final int MAX_VALUE = Integer.MAX_VALUE;

  // key: computationURI, value: results extended from Array.
  // Used to store the results of a map computation for a given URI
  private Map<String, ArrayList<?>> mapResults = new HashMap<>();

  private int minHash;
  private int maxHash;

  private DHTNode next; // CANNOT BE NULL
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  protected DHTContentAccessInboundPort contentAccessInboudPort;
  public static final String CONTENT_ACCESS_INBOUND_PORT_URI = "content-access-inbound-port-uri";

  protected DHTMapReduceInboundPort mapReduceInboundPort;
  public static final String MAP_REDUCE_INBOUND_PORT_URI = "map-reduce-inbound-port-uri";

  protected DHTNode() {
    super(1, 0);
    this.minHash = Integer.MIN_VALUE;
    this.maxHash = Integer.MAX_VALUE;
    this.next = this;
    try {
      this.contentAccessInboudPort = new DHTContentAccessInboundPort(CONTENT_ACCESS_INBOUND_PORT_URI, this);
      this.contentAccessInboudPort.publishPort();

      this.mapReduceInboundPort = new DHTMapReduceInboundPort(MAP_REDUCE_INBOUND_PORT_URI, this);
      this.mapReduceInboundPort.publishPort();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }

  @Override
  public synchronized void shutdown() throws ComponentShutdownException {
    try {
      this.contentAccessInboudPort.unpublishPort();
    } catch (Exception e) {
      throw new ComponentShutdownException(e);
    }
    super.shutdown();
  }

  public static boolean isBetween(int value, int min, int max) {
    return value >= min && value < max;
  }

  private boolean isBetween(int value) {
    return isBetween(value, minHash, maxHash);
  }

  private void checkMerge() {
    if (this.localStorage.size() < MAX_VALUE / 3 && this.next.localStorage.size() < MAX_VALUE / 3) {
      this.merge();
    }
  }

  private void merge() {
    if (this.next == this) {
      return;
    }

    // merge data
    this.localStorage.putAll(this.next.localStorage);
    this.next.localStorage.clear();

    // merge hash
    this.maxHash = this.next.maxHash;
    this.next = this.next.next;
  }

  private void checkSplit() {
    if (this.localStorage.size() > MAX_VALUE) {
      // this.splitNode();
    }
  }

  /*
   * private void splitNode() {
   * int minHashValue = this.maxHash;
   * int maxHashValue = this.minHash;
   * 
   * // find min and max hash
   * for (ContentKeyI key : localStorage.keySet()) {
   * int hash = key.hashCode();
   * minHashValue = Math.min(minHashValue, hash);
   * maxHashValue = Math.max(maxHashValue, hash);
   * }
   * 
   * // spliting on the middle of the range of hash values
   * // When working with integer keys, hash values tends to be close to each
   * other,
   * // that's why we use this method instead of just cutting in half the range
   * // Note: Using long to avoid overflow
   * int newMinHash = (int) Math.floorDiv((long) maxHashValue + (long)
   * maxHashValue, (long) 2);
   * int newMaxHash = this.maxHash;
   * 
   * this.maxHash = newMinHash - 1;
   * this.next = new DHTNode(next, newMinHash, newMaxHash);
   * 
   * // Move data
   * Map<ContentKeyI, ContentDataI> newLocalStorage = new HashMap<>();
   * this.localStorage.entrySet().removeIf(entry -> {
   * if (entry.getKey().hashCode() > this.maxHash) {
   * newLocalStorage.put(entry.getKey(), entry.getValue());
   * return true;
   * }
   * return false;
   * });
   * 
   * this.next.localStorage.putAll(newLocalStorage);
   * }
   */

  public String toString() {
    return "DHTNode [minHash=" + this.minHash + ", maxHash=" + this.maxHash + ", nbElements=" + this.localStorage.size()
        + "]";
  }

  public void printChain(String URI) {
    if (this.seenPrintURIs.contains(URI)) {
      System.out.println("INFO PRINTCHAIN loop detected (or called before previous computation ends)");
      return;
    }
    this.seenPrintURIs.add(URI);
    System.out.println("Node: " + this);
    System.out.println("Data: " + localStorage);
    System.out.println();
    this.next.printChain(URI);
    this.seenPrintURIs.remove(URI);
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return this.next.getSync(URI, key);

    return this.localStorage.get(key);
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return this.next.putSync(URI, key, value);

    ContentDataI out = this.localStorage.put(key, value);
    this.checkSplit();
    return out;
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    if (!this.isBetween(key.hashCode()))
      return this.next.removeSync(URI, key);

    ContentDataI out = this.localStorage.remove(key);
    this.checkMerge();
    return out;
  }

  @Override
  public void clearComputation(String URI) throws Exception {
  }

  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    this.seenURIs.remove(URI);
    this.mapResults.remove(URI);
  }

  @Override
  public <R extends Serializable> void mapSync(String URI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    if (this.seenURIs.contains(URI)) {
      System.out.println("INFO MAPSYNC loop detected (or called before previous computation ends) with URI" + URI);
      return;
    }
    this.seenURIs.add(URI);

    ArrayList<R> results = this.localStorage.values().stream()
        .filter(selector::test)
        .map(processor::apply)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    this.mapResults.put(URI, results);
    this.next.mapSync(URI, selector, processor);
    this.seenURIs.remove(URI);
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator,
      A acc)
      throws Exception {
    if (this.seenURIs.contains(URI)) {
      System.out
          .println("INFO REDUCESYNC loop detected (or called before previous computation ends) with URI" + URI);
      return acc;
    }
    this.seenURIs.add(URI);

    // HACK Il faut vérifier si le cast est possible
    @SuppressWarnings("unchecked")
    ArrayList<R> data = (ArrayList<R>) this.mapResults.get(URI);
    if (data == null) {
      throw new Exception("No data found for URI " + URI);
    }

    A currentResult = data.stream().reduce(acc, reductor::apply, combinator::apply);

    A nextResult = next.reduceSync(URI, reductor, combinator, acc);
    currentResult = combinator.apply(currentResult, nextResult);

    this.seenURIs.remove(URI);
    return currentResult;
  }
}
