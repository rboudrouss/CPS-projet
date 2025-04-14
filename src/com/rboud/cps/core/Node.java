package com.rboud.cps.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.rboud.cps.utils.MyInterval;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
public class Node extends AbstractComponent implements ContentAccessSyncI, MapReduceSyncI {
  private Set<String> seenURIs = new HashSet<String>();

  // key: computationURI, value: results extended from Array.
  // Used to store the results of a map computation for a given URI
  private Map<String, Stream<?>> mapResults = new HashMap<>();

  // Describe the range of hash values that this node is responsible for
  private MyInterval interval;

  // Storage
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  // Ports URIS
  public static final String URI_PREFIX = "node-";

  // Composite endpoint for connecting to the Facade, may be null
  ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nodeFacadeCompositeEndpoint;

  // Composite endpoint for connecting to next node in chain, both CANNOT BE NULL
  ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> selfNodeCompositeEndpoint;
  ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nextNodeCompositeEndpoint;

  protected Node(
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nextNodeCompositeEndpoint)
      throws Exception {
    super(1, 0);

    assert selfNodeCompositeEndpoint != null;
    assert nextNodeCompositeEndpoint != null;

    this.interval = new MyInterval(Integer.MIN_VALUE, Integer.MAX_VALUE);
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.selfNodeCompositeEndpoint = selfNodeCompositeEndpoint;
    this.nextNodeCompositeEndpoint = nextNodeCompositeEndpoint;

    this.selfNodeCompositeEndpoint.initialiseServerSide(this);
    if (this.nodeFacadeCompositeEndpoint != null) {
      this.nodeFacadeCompositeEndpoint.initialiseServerSide(this);
    }

    this.toggleLogging();
    this.toggleTracing();
  }

  protected Node(
    ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nextNodeCompositeEndpoint,
      int minValue, int maxValue) throws Exception {
    this(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
    this.interval = new MyInterval(minValue, maxValue);
  }

  // ------------------------------------------------------------------------
  // Component lifecycle methods
  // ------------------------------------------------------------------------

  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[NODE] Starting DHT Node component : " + this);
    try {
      this.nextNodeCompositeEndpoint.initialiseClientSide(this);
    } catch (Throwable e) {
      throw new ComponentStartException(e);
    }
    super.start();
  }

  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[NODE] Finalising DHT Node component.");
    this.printExecutionLogOnFile("logs/dht-node");

    if (this.nodeFacadeCompositeEndpoint != null) {
      this.nodeFacadeCompositeEndpoint.cleanUpServerSide();
    }
    this.selfNodeCompositeEndpoint.cleanUpServerSide();
    this.nextNodeCompositeEndpoint.cleanUpClientSide();
    super.finalise();
  }

  // ------------------------------------------------------------------------
  // DHTNode methods (ContentAccessSyncI, MapReduceSyncI)
  // ------------------------------------------------------------------------

  // ContentAccessSyncI ----------------------
  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    this.logMessage("[NODE] Getting content with key: " + key + " and URI: " + URI);
    if (!this.interval.in(key.hashCode()))
      return this.getNextContentAccessReference().getSync(URI, key);

    return this.localStorage.get(key);
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    this.logMessage("[NODE] Putting content with key: " + key + " and value: " + value + " and URI: " + URI);
    if (!this.interval.in(key.hashCode()))
      return this.getNextContentAccessReference().putSync(URI, key, value);

    ContentDataI out = this.localStorage.put(key, value);
    return out;
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    this.logMessage("[NODE] Removing content with key: " + key + " and URI: " + URI);
    if (!this.interval.in(key.hashCode()))
      return this.getNextContentAccessReference().removeSync(URI, key);

    ContentDataI out = this.localStorage.remove(key);
    return out;
  }

  @Override
  public void clearComputation(String URI) throws Exception {
  }

  // MapReduceSyncI ----------------------

  @Override
  public <R extends Serializable> void mapSync(String URI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.logMessage("[NODE] Mapping with URI: " + URI);
    if (this.mapResults.containsKey(URI)) {
      this.logMessage("INFO MAPSYNC loop detected With URI " + URI);
      return;
    }

    Stream<R> results = this.localStorage.values().stream()
        .filter(selector)
        .map(processor);

    this.mapResults.put(URI, results);
    this.getNextMapReduceReference().mapSync(URI, selector, processor);
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator,
      A acc)
      throws Exception {
    this.logMessage("[NODE] Reducing with URI: " + URI);
    if (this.seenURIs.contains(URI)) {
      this.logMessage("INFO REDUCESYNC loop detected With URI " + URI);
      return acc;
    }
    this.seenURIs.add(URI);

    // HACK Il faut vérifier si le cast est possible
    @SuppressWarnings("unchecked")
    Stream<R> data = (Stream<R>) this.mapResults.get(URI);
    if (data == null) {
      throw new Exception("No data found for URI " + URI);
    }

    A currentResult = data.reduce(acc, reductor, combinator);

    A nextResult = this.getNextMapReduceReference().reduceSync(URI, reductor, combinator, acc);
    currentResult = combinator.apply(currentResult, nextResult);

    this.seenURIs.remove(URI);
    return currentResult;
  }

  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    if (!this.mapResults.containsKey(URI)) return;
      this.mapResults.get(URI).close();
      this.mapResults.remove(URI);
      this.getNextMapReduceReference().clearMapReduceComputation(URI);
  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  public static boolean isBetween(int value, int min, int max) {
    return value >= min && value < max;
  }

  public String toString() {
    return "DHTNode [minHash=" + this.interval.first() + ", maxHash=" + this.interval.last() + ", nbElements="
        + this.localStorage.size()
        + "]";
  }

  private ContentAccessSyncI getNextContentAccessReference() {
    return this.nextNodeCompositeEndpoint.getContentAccessEndpoint().getClientSideReference();
  }

  private MapReduceSyncI getNextMapReduceReference() {
    return this.nextNodeCompositeEndpoint.getMapReduceEndpoint().getClientSideReference();
  }
}
