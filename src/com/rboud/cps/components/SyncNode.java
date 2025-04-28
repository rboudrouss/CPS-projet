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

/**
 * Represents a synchronous node in a distributed content management and
 * MapReduce system.
 * This class implements both MapReduceSync and ContentAccessSync interfaces to
 * provide
 * synchronous operations for distributed computing.
 * 
 * The node maintains a map for storing content data and map operation results,
 * supporting synchronous execution of map-reduce operations across a ring of
 * nodes.
 * 
 * 
 * Key features:
 * - Synchronous content management (get, put, remove)
 * - Distributed map-reduce operations
 * - Ring-based node organization
 * - Hash-based content distribution
 * - Node-to-node communication
 * 
 * The node uses an interval mechanism to determine which content
 * it is responsible
 * for storing based on hash values. Content operations are
 * forwarded to the appropriate
 * node in the ring if the current node is not responsible for the
 * given key.
 * 
 * Implementation notes:
 * - Uses HashMap for content storage
 * - Implements synchronous request handling
 * - Maintains node interval boundaries for content distribution
 * - Supports distributed map-reduce operations with result
 * aggregation
 * 
 * @param <CAI> The interface type for content access operations
 * @param <MRI> The interface type for map-reduce operations
 * 
 * @see ContentAccessSyncI
 * @see MapReduceSyncI
 * @see HashMap
 * @see MyInterval
 */
@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
public class SyncNode<CAI extends ContentAccessSyncI, MRI extends MapReduceSyncI> extends AbstractComponent
    implements ContentAccessSyncI, MapReduceSyncI {

  /** Minimum number of threads required for handling concurrent requests */
  protected static final int MIN_THREADS = 2;
  /** Minimum number of schedulable threads */
  protected static final int MIN_SCHEDULABLE_THREADS = 0;

  /** URI prefix for node identification */
  public static final String URI_PREFIX = "node-";

  /** Unique identifier for this node */
  protected String nodeURI;

  /** Storage for map computation results keyed by computation URI */
  protected Map<String, Stream<?>> mapSyncResults = new HashMap<>();

  /** Range of hash values this node is responsible for */
  protected MyInterval interval;

  /** Local storage for content data */
  protected Map<ContentKeyI, ContentDataI> localStorage;

  /** Composite endpoint for facade connection, can be null */
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint;

  /**
   * Composite endpoint for next node in chain of which this node is the server,
   * cannot be null
   */
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint;

  /**
   * Composite endpoint for next node in chain of which this node is the client,
   * cannot be null
   */
  protected ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint;

  /**
   * Creates a new SyncNode with specified parameters.
   *
   * @param nbthreads                   Number of threads
   * @param nbschedulablethreads        Number of schedulable threads
   * @param nodeFacadeCompositeEndpoint Endpoint for facade connection
   * @param selfNodeCompositeEndpoint   Endpoint for this node
   * @param nextNodeCompositeEndpoint   Endpoint for next node
   * @param minValue                    Minimum hash value this node handles
   * @param maxValue                    Maximum hash value this node handles
   *
   * @throws Exception if BCM initialization fails or server initialization in
   *                   endpoints fails
   */
  protected SyncNode(
      int nbthreads,
      int nbschedulablethreads,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint,
      int minValue, int maxValue) throws Exception {

    super(nbthreads, nbschedulablethreads);

    assert selfNodeCompositeEndpoint != null;
    assert nextNodeCompositeEndpoint != null;

    this.nodeURI = URIGenerator.generateURI(URI_PREFIX);

    this.initialise(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint, minValue,
        maxValue);
    assert this.localStorage != null : "localStorage is null";
  }

  /**
   * Creates a new SyncNode with specified parameters.
   *
   * @param nbthreads                   Number of threads
   * @param nbschedulablethreads        Number of schedulable threads
   * @param nodeFacadeCompositeEndpoint Endpoint for facade connection
   * @param selfNodeCompositeEndpoint   Endpoint for this node
   * @param nextNodeCompositeEndpoint   Endpoint for next node
   * 
   * @throws Exception if BCM initialization fails or server initialization in
   *                   endpoints fails
   */
  protected SyncNode(
      int nbthreads,
      int nbschedulablethreads,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint)
      throws Exception {
    //
    this(nbthreads, nbschedulablethreads, nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint,
        nextNodeCompositeEndpoint, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  /**
   * Creates a new SyncNode with specified parameters.
   *
   * @param nodeFacadeCompositeEndpoint Endpoint for facade connection
   * @param selfNodeCompositeEndpoint   Endpoint for this node
   * @param nextNodeCompositeEndpoint   Endpoint for next node
   * @throws Exception if BCM initialization fails or server initialization in
   *                   endpoints fails
   */
  protected SyncNode(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint)
      throws Exception {
    this(2, 0, nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
  }

  /**
   * Creates a new SyncNode with specified parameters.
   *
   * @param nodeFacadeCompositeEndpoint Endpoint for facade connection
   * @param selfNodeCompositeEndpoint   Endpoint for this node
   * @param nextNodeCompositeEndpoint   Endpoint for next node
   * @param minValue                    Minimum hash value this node handles
   * @param maxValue                    Maximum hash value this node handles
   * @throws Exception if BCM initialization fails or server initialization in
   *                   endpoints fails
   */
  protected SyncNode(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint,
      int minValue, int maxValue)
      throws Exception {
    // 2 threads are necessary for answering the request where this node is blocked
    // by the facade request and gets an new request from a node. This is because
    // in the inbount ports we used handleRequest an not calling the method
    // directly.
    // so the first method call is not handled by the facade thread.
    this(2, 0, nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint, minValue, maxValue);
  }

  // ------------------------------------------------------------------------
  // Initialisation methods
  // ------------------------------------------------------------------------

  /**
   * Initializes server-side connections for this node.
   * 
   *
   * @throws Exception if server initialization fails
   */
  protected void initialiseServerConnection() throws Exception {
    this.selfNodeCompositeEndpoint.initialiseServerSide(this);
    if (this.nodeFacadeCompositeEndpoint != null) {
      this.nodeFacadeCompositeEndpoint.initialiseServerSide(this);
    }
  }

  /**
   * 
   * Initialise the node, the parameters are those passed by the constructor.
   * Then initialise the server connection.
   * finally, the logging and tracing are toggled.
   * 
   * <!> Do not forget to initialise this.localStorage !!
   * 
   * @param nodeFacadeCompositeEndpoint The endpoint for the facade
   * @param selfNodeCompositeEndpoint   The endpoint for the next node were this
   *                                    node is the server, initialise the inbound
   *                                    ports
   * @param nextNodeCompositeEndpoint   The endpoint for the next node were this
   *                                    node is the client, initialise the
   *                                    outbound ports
   * @param minValue                    the minimum value of the range of hash
   *                                    values that this node is responsible for
   * @param maxValue                    the maximum value of the range of hash
   *                                    values that this node is responsible for
   * 
   * @throws Exception if server initialization in endpoints fails
   */
  protected void initialise(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint,
      int minValue,
      int maxValue) throws Exception {
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.selfNodeCompositeEndpoint = selfNodeCompositeEndpoint;
    this.nextNodeCompositeEndpoint = nextNodeCompositeEndpoint;
    this.interval = new MyInterval(minValue, maxValue);
    this.localStorage = new HashMap<>();

    this.initialiseServerConnection();

    this.toggleLogging();
    this.toggleTracing();
    this.getTracer().setTitle("Node " + this.nodeURI);
  }

  // ------------------------------------------------------------------------
  // Component lifecycle methods
  // ------------------------------------------------------------------------

  /**
   * 
   * Starts the Node component and initializes the client-side connection to next
   * node
   * 
   * @see AbstractComponent#start()
   */
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

  /**
   * 
   * Shuts down the Node component and cleans up connections
   * also prints the execution log to a file.
   * 
   * @see AbstractComponent#shutdown()
   */
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

  /**
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see ContentAccessSyncI#getSync(String, ContentKeyI)
   */
  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    this.logMessage("[NODE] Getting content with key: " + key + " and URI: " + URI);
    if (!this.interval.in(key.hashCode()))
      return this.getNextContentAccessReference().getSync(URI, key);

    return this.localStorage.get(key);
  }

  /**
   * 
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see ContentAccessSyncI#putSync(String, ContentKeyI, ContentDataI)
   */
  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    this.logMessage("[NODE] Putting content with key: " + key + " and value: " + value + " and URI: " + URI);
    if (!this.interval.in(key.hashCode()))
      return this.getNextContentAccessReference().putSync(URI, key, value);

    ContentDataI out = this.localStorage.put(key, value);
    return out;
  }

  /**
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see ContentAccessSyncI#removeSync(String, ContentKeyI)
   */
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

  /**
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see MapReduceSyncI#mapSync(String, SelectorI, ProcessorI)
   */
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

  /**
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see MapReduceSyncI#reduceSync(String, ReductorI, CombinatorI, A)
   */
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

  /**
   * @throws Exception may throw an exception if smth external happens, like
   *                   interruption
   * 
   * @see MapReduceSyncI#clearMapReduceComputation(String)
   */
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

  /**
   * Returns a string representation of this node.
   *
   * @return String containing node information
   */
  @Override
  public String toString() {
    return "DHTNode [minHash=" + this.interval.first() + ", maxHash=" + this.interval.last() + ", nbElements="
        + this.localStorage.size()
        + "]";
  }

  /**
   * Gets the content access reference to the next node.
   *
   * @return Reference to next node's content access interface
   */
  protected CAI getNextContentAccessReference() {
    return this.nextNodeCompositeEndpoint.getContentAccessEndpoint().getClientSideReference();
  }

  /**
   * Gets the map-reduce reference to the next node.
   *
   * @return Reference to next node's map-reduce interface
   */
  protected MRI getNextMapReduceReference() {
    return this.nextNodeCompositeEndpoint.getMapReduceEndpoint().getClientSideReference();
  }
}
