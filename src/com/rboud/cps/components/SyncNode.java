package com.rboud.cps.components;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
public class SyncNode<CAI extends ContentAccessSyncI, MRI extends MapReduceSyncI> extends AbstractComponent
    implements ContentAccessSyncI, MapReduceSyncI {

  // URIs prefix
  public static final String URI_PREFIX = "node-";

  // String id
  protected String nodeURI;

  // key: computationURI, value: results extended from Array.
  // Used to store the results of a map computation for a given URI
  protected Map<String, Stream<?>> mapSyncResults = new HashMap<>();

  // Describe the range of hash values that this node is responsible for
  protected MyInterval interval;

  // Storage
  protected final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  // Composite endpoint for connecting to the Facade, may be null
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint;

  // Composite endpoint for connecting to next node in chain, both CANNOT BE NULL
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint;
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint;

  protected SyncNode(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint)
      throws Exception {
    // 2 threads are necessary for answering the request where this node is blocked
    // by the facade request and gets an new request from a node. This is because
    // in the inbount ports we used handleRequest an not calling the method
    // directly.
    // so the first method call is not handled by the facade thread.
    super(2, 0);

    assert selfNodeCompositeEndpoint != null;
    assert nextNodeCompositeEndpoint != null;

    this.nodeURI = URIGenerator.generateURI(URI_PREFIX);

    this.interval = new MyInterval(Integer.MIN_VALUE, Integer.MAX_VALUE);
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.selfNodeCompositeEndpoint = selfNodeCompositeEndpoint;
    this.nextNodeCompositeEndpoint = nextNodeCompositeEndpoint;

    this.initialiseConnection();
  }

  protected SyncNode(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint,
      int minValue, int maxValue) throws Exception {
    this(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
    this.interval = new MyInterval(minValue, maxValue);
  }

  private void initialiseConnection() throws Exception {
    this.selfNodeCompositeEndpoint.initialiseServerSide(this);
    if (this.nodeFacadeCompositeEndpoint != null) {
      this.nodeFacadeCompositeEndpoint.initialiseServerSide(this);
    }

    this.toggleLogging();
    this.toggleTracing();
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
  public synchronized void shutdown() throws ComponentShutdownException {
    this.logMessage("[NODE] Finalising DHT Node component.");
    try {
      this.printExecutionLogOnFile("logs/node-" + this.nodeURI);
    } catch (FileNotFoundException e) {
      throw new ComponentShutdownException(e);
    }

    if (this.nodeFacadeCompositeEndpoint != null) {
      this.nodeFacadeCompositeEndpoint.cleanUpServerSide();
    }
    this.selfNodeCompositeEndpoint.cleanUpServerSide();
    this.nextNodeCompositeEndpoint.cleanUpClientSide();
    super.shutdown();
  }

  // ------------------------------------------------------------------------
  // DHTNode sync methods (ContentAccessSyncI, MapReduceSyncI)
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
    if (this.mapSyncResults.containsKey(URI)) {
      this.logMessage("INFO MAPSYNC loop detected With URI " + URI);
      return;
    }

    this.logMessage("[NODE] executing map on local storage.");
    Stream<R> results = this.localStorage.values().stream()
        .filter(selector)
        .map(processor);

    this.mapSyncResults.put(URI, results);

    this.logMessage("[NODE] sending to next node.");
    this.getNextMapReduceReference().mapSync(URI, selector, processor);

    this.logMessage("[NODE] Map finished.");
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator,
      A acc)
      throws Exception {
    this.logMessage("[NODE] Reducing with URI: " + URI);
    if (!this.mapSyncResults.containsKey(URI)) {
      this.logMessage("[NODE] INFO REDUCESYNC, nothing in mapresults, maybe a loop detected With URI " + URI);
      return acc;
    }

    // HACK Il faut vérifier si le cast est possible
    Stream<R> data = (Stream<R>) this.mapSyncResults.get(URI);
    this.mapSyncResults.remove(URI);

    A currentResult = data.reduce(acc, reductor, combinator);
    data.close();

    A nextResult = this.getNextMapReduceReference().reduceSync(URI, reductor, combinator, acc);
    currentResult = combinator.apply(currentResult, nextResult);

    return currentResult;
  }

  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    if (!this.mapSyncResults.containsKey(URI))
      return;
    this.mapSyncResults.get(URI).close();
    this.mapSyncResults.remove(URI);
    this.getNextMapReduceReference().clearMapReduceComputation(URI);
  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  public String toString() {
    return "DHTNode [minHash=" + this.interval.first() + ", maxHash=" + this.interval.last() + ", nbElements="
        + this.localStorage.size()
        + "]";
  }

  protected CAI getNextContentAccessReference() {
    return this.nextNodeCompositeEndpoint.getContentAccessEndpoint().getClientSideReference();
  }

  protected MRI getNextMapReduceReference() {
    return this.nextNodeCompositeEndpoint.getMapReduceEndpoint().getClientSideReference();
  }
}
