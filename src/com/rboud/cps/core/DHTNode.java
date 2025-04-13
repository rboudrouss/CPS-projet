package com.rboud.cps.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;

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
import fr.sorbonne_u.components.exceptions.ComponentStartException;

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class DHTNode extends AbstractComponent implements ContentAccessSyncI, MapReduceSyncI {
  private Set<String> seenURIs = new HashSet<String>();

  // max element in node, Must be >= 2
  public static final int MAX_VALUE = Integer.MAX_VALUE;

  // key: computationURI, value: results extended from Array.
  // Used to store the results of a map computation for a given URI
  private Map<String, ArrayList<?>> mapResults = new HashMap<>();

  // Describe the range of hash values that this node is responsible for
  private int minHash;
  private int maxHash;

  // Pointer to the next node in the ring
  private DHTNode next; // CANNOT BE NULL

  // Storage
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  // Ports URIS
  public static final String CONTENT_ACCESS_INBOUND_PORT_URI = "content-access-inbound-port-uri";
  public static final String MAP_REDUCE_INBOUND_PORT_URI = "map-reduce-inbound-port-uri";

  NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint;

  protected DHTNode(NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint) throws Exception {
    super(1, 0);
    this.minHash = Integer.MIN_VALUE;
    this.maxHash = Integer.MAX_VALUE;
    this.next = this;
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;

    try {
      this.nodeFacadeCompositeEndpoint.initialiseServerSide(this);
    } catch (Exception e) {
      throw new Exception(e);
    }

    this.toggleLogging();
    this.toggleTracing();
  }

  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[NODE] Starting DHT Node component : " + this);
    super.start();
  }

  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[NODE] Finalising DHT Node component.");
    this.printExecutionLogOnFile("logs/dht-node");

    this.nodeFacadeCompositeEndpoint.cleanUpServerSide();
    super.finalise();
  }

  public static boolean isBetween(int value, int min, int max) {
    return value >= min && value < max;
  }

  private boolean isBetween(int value) {
    return isBetween(value, minHash, maxHash);
  }

  public String toString() {
    return "DHTNode [minHash=" + this.minHash + ", maxHash=" + this.maxHash + ", nbElements=" + this.localStorage.size()
        + "]";
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    this.logMessage("[NODE] Getting content with key: " + key + " and URI: " + URI);
    if (!this.isBetween(key.hashCode()))
      return this.next.getSync(URI, key);

    return this.localStorage.get(key);
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    this.logMessage("[NODE] Putting content with key: " + key + " and value: " + value + " and URI: " + URI);
    if (!this.isBetween(key.hashCode()))
      return this.next.putSync(URI, key, value);

    ContentDataI out = this.localStorage.put(key, value);
    return out;
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    this.logMessage("[NODE] Removing content with key: " + key + " and URI: " + URI);
    if (!this.isBetween(key.hashCode()))
      return this.next.removeSync(URI, key);

    ContentDataI out = this.localStorage.remove(key);
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
    this.logMessage("[NODE] Mapping with URI: " + URI);
    if (this.seenURIs.contains(URI)) {
      System.out.println("INFO MAPSYNC loop detected (or called before previous computation ends) with URI" + URI);
      return;
    }
    this.seenURIs.add(URI);

    ArrayList<R> results = this.localStorage.values().stream()
        .filter(selector)
        .map(processor)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    this.mapResults.put(URI, results);
    this.next.mapSync(URI, selector, processor);
    this.seenURIs.remove(URI);
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator,
      A acc)
      throws Exception {
    this.logMessage("[NODE] Reducing with URI: " + URI);
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

    A currentResult = data.stream().reduce(acc, reductor, combinator);

    A nextResult = next.reduceSync(URI, reductor, combinator, acc);
    currentResult = combinator.apply(currentResult, nextResult);

    this.seenURIs.remove(URI);
    return currentResult;
  }

}
