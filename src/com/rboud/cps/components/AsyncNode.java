package com.rboud.cps.components;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.rboud.cps.utils.URIStamper;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * Represents an asynchronous node in a distributed content management and
 * MapReduce system. This class extends SyncNode and implements both MapReduce
 * and ContentAccess interfaces to provide asynchronous operations for
 * distributed computing. The node maintains a concurrent hash map for storing
 * map operation results and supports asynchronous execution of map-reduce
 * operations across a ring of nodes.
 * 
 * Key features:
 * - Asynchronous content management (get, put, remove)
 * - Distributed map-reduce operations
 * - Concurrent operation handling with dedicated thread pools:
 * - Content access pool (1 thread by default for fast operations)
 * - MapReduce pool (2 threads by default for parallel processing)
 * - Node-to-node communication
 * - Result forwarding capabilities
 * - Parallel stream processing for map operations
 * 
 * The node uses a URI stamping mechanism to track the origin of computations
 * and prevent infinite loops in the distributed network in map-reduce
 * operations.
 * 
 * Implementation notes:
 * - Heavily relies on ConcurrentHashMap for thread-safe storage
 * - Uses CompletableFuture for asynchronous result handling (pausing reduce
 * till map is done)
 * - Utilizes a custom URI stamping mechanism to manage computation URIs (The
 * origin node is stamped in the URI)
 * - Supports distributed map-reduce operations with result aggregation
 * - Separate executor services for content access and map-reduce tasks
 * 
 * @param <ContentAccessI> The interface type for content access operations
 * @param <MapReduceI>     The interface type for map-reduce operations
 * 
 * @see SyncNode
 * @see MapReduceI
 * @see ContentAccessI
 * @see ConcurrentHashMap
 * @see CompletableFuture
 */
@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
    MapReduceResultReceptionCI.class })
public class AsyncNode extends SyncNode<ContentAccessI, MapReduceI>
    implements MapReduceI, ContentAccessI {

  /** the default number of threads used to run the async node */
  public final static int DEFAULT_NB_THREADS = 1;

  /** the default number of threads that can be scheduled */
  public final static int DEFAULT_NB_SCHEDULABLE_THREADS = 0;

  /** the default number of threads in the Content Access pool */
  public final static int DEFAULT_NB_CONTENT_ACCESS_THREADS = 1;

  /** the default number of threads in the MapReduce pool */
  public final static int DEFAULT_NB_MAP_REDUCE_THREADS = 2;

  /** the prefix for executor services uris */
  public static final String EXECUTOR_SERVICE_URI_PREFIX = "ASYNCNODE-EXS-";

  /** the prefix for content access executor services uris */
  private String contentAccessExecutorServiceURI;

  /** the prefix for map reduce executor services uris */
  private String mapReduceExecutorServiceURI;

  /**
   * ConcurrentHashMap for storing the results of map operations.
   * Used primarily for putting/getting futures in the map-reduce
   */
  protected ConcurrentHashMap<String, CompletableFuture<Stream<?>>> mapResults;

  /**
   * Creates an AsyncNode with specified range values and thread configurations.
   *
   * @param nbthreads                   The number of threads for the component
   * @param nbSchedulableThreads        The number of schedulable threads
   * @param nodeFacadeCompositeEndpoint The composite endpoint representing the
   *                                    node facade
   * @param selfNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the server,
   * @param nextNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the client
   * @param nbContentAccessThreads      The number of threads for content access
   *                                    operations
   * @param nbMapRedueThreads           The number of threads for map-reduce
   *                                    operations
   * @param minValue                    The minimum value handled by this node
   * @param maxValue                    The maximum value handled by this node
   * 
   * @throws Exception If there is an error during BCM component creation or
   *                   server initialization in endpoints
   * 
   * @see ContentNodeBaseCompositeEndPointI
   * @see ContentAccessI
   * @see MapReduceI
   */
  protected AsyncNode(int nbthreads, int nbSchedulableThreads,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nextNodeCompositeEndpoint,
      int minValue, int maxValue) throws Exception {
    super(nbthreads, nbSchedulableThreads, nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint,
        nextNodeCompositeEndpoint, minValue, maxValue);

    assert this.localStorage instanceof ConcurrentHashMap : "Local storage should be a ConcurrentHashMap";
    assert this.mapResults instanceof ConcurrentHashMap : "Map results should be a ConcurrentHashMap";
    assert this.mapReduceExecutorServiceURI != null : "MapReduce executor service URI should not be null";
    assert this.contentAccessExecutorServiceURI != null : "Content access executor service URI should not be null";

  }

  /**
   * Creates an AsyncNode with specified range values.
   *
   * @param nodeFacadeCompositeEndpoint The composite endpoint representing the
   *                                    node facade
   * @param selfNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the server,
   * @param nextNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the client
   * @param minValue                    The minimum value handled by this node
   * @param maxValue                    The maximum value handled by this node
   * 
   * @throws Exception If there is an error during BCM component creation or
   *                   server initialization in endpoints
   * 
   * @see ContentNodeBaseCompositeEndPointI
   * @see ContentAccessI
   * @see MapReduceI
   */
  protected AsyncNode(ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nextNodeCompositeEndpoint,
      int minValue, int maxValue) throws Exception {
    this(
        DEFAULT_NB_THREADS, DEFAULT_NB_SCHEDULABLE_THREADS, nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint,
        nextNodeCompositeEndpoint, minValue,
        maxValue);
  }

  /**
   * Creates an AsyncNode with default range values.
   * 
   * @param nodeFacadeCompositeEndpoint The composite endpoint representing the
   *                                    node facade
   * @param selfNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the server,
   * @param nextNodeCompositeEndpoint   Composite endpoint for next node in chain
   *                                    of which this node is the client
   * @throws Exception If there is an error during BCM component creation or
   *                   server initialization in endpoints
   */
  protected AsyncNode(ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nextNodeCompositeEndpoint) throws Exception {
    this(
        nodeFacadeCompositeEndpoint,
        selfNodeCompositeEndpoint,
        nextNodeCompositeEndpoint,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void initialise(ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nextNodeCompositeEndpoint, int minValue,
      int maxValue) throws Exception {
    this.contentAccessExecutorServiceURI = URIGenerator.generateURI(EXECUTOR_SERVICE_URI_PREFIX);
    this.mapReduceExecutorServiceURI = URIGenerator.generateURI(EXECUTOR_SERVICE_URI_PREFIX);

    this.createNewExecutorService(contentAccessExecutorServiceURI, DEFAULT_NB_CONTENT_ACCESS_THREADS, false);
    this.createNewExecutorService(mapReduceExecutorServiceURI, DEFAULT_NB_MAP_REDUCE_THREADS, false);

    super.initialise(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint,
        nextNodeCompositeEndpoint, minValue, maxValue);

    this.localStorage = new ConcurrentHashMap<>();
    this.mapResults = new ConcurrentHashMap<>();
  }

  // ------------------------------------------------------------------------
  // Content Access methods
  // ------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   * 
   * @throws Exception if there is an error during port connection or if smth
   *                   external happens, for example interrupted
   * 
   * @see {@link #sendResult(String, EndPointI, String, Serializable)}
   */
  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.logMessage("[NODE-GET] Getting content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().get(computationURI, key, caller);
      return;
    }

    this.sendResult("GET", caller, computationURI, this.localStorage.get(key));
  }

  /**
   * {@inheritDoc}
   * 
   * @throws Exception if there is an error during port connection or if smth
   *                   external happens, for example interrupted
   * 
   * @see {@link #sendResult(String, EndPointI, String, Serializable)}
   */
  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    this.logMessage("[NODE-PUT] Putting content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().put(computationURI, key, value, caller);
      return;
    }
    this.sendResult("PUT", caller, computationURI, this.localStorage.put(key, value));
    this.logMessage("\n[NODE-PUT] New hashmap content: " + this.localStorage);
  }

  /**
   * {@inheritDoc}
   * 
   * @throws Exception if there is an error during port connection or if smth
   *                   external happens, for example interrupted
   * 
   * @see {@link #sendResult(String, EndPointI, String, Serializable)}
   */
  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.logMessage("[NODE-REMOVE] Removing content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().remove(computationURI, key, caller);
      return;
    }

    this.sendResult("REMOVE", caller, computationURI, this.localStorage.remove(key));
    this.logMessage("\n[NODE-REMOVE] New hashmap content: " + this.localStorage);
  }

  // ------------------------------------------------------------------------
  // MapReduce methods
  // ------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   * 
   * Just in case but normally there shouldn't be any residual data
   */
  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    mapResults.compute(URI, (uri, existingFuture) -> {
      if (existingFuture != null) {
        existingFuture.cancel(true);
        try {
          this.getNextMapReduceReference().clearMapReduceComputation(URI);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      return null;
    });
    super.clearMapReduceComputation(URI);
  }

  /**
   * {@inheritDoc}
   * 
   * @throws Exception if there is an error during port connection or if smth
   *                   external happens, for example interrupted
   * 
   * 
   * @see {@link #sendResult(String, EndPointI, String, String, Serializable)}
   * @see URIStamper
   */
  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    // Using computationURI as the key as URIStamper's method is not thread safe. so
    // the first node will store the data in a different uri than the others but it
    // doesn't matter
    mapResults.compute(computationURI, (uri, existingFuture) -> {
      this.logMessage("[NODE-MAP] Calling map with uri " + computationURI);

      if (this.nodeURI.equals(URIStamper.getOriginNodeFromUri(computationURI))) {
        this.logMessage("[NODE-MAP] Looped map !");
        return existingFuture;
      }

      if (existingFuture != null) {
        this.logMessage("[NODE-MAP] MAP WARNING : smh got called but value already existing.");
        return existingFuture;
      }

      String newUri = URIStamper.isComputationUriStamped(computationURI) ? computationURI
          : URIStamper.stampOriginNodeToUri(computationURI, this.nodeURI);

      try {
        this.getNextMapReduceReference().map(newUri, selector, processor);
      } catch (Exception e) {
        this.logMessage("[NODE-MAP] MAP ERROR sending to next node");
        e.printStackTrace();
      }

      return CompletableFuture.supplyAsync(() -> this.localStorage.values().stream().parallel()
          .filter(selector)
          .map(processor));
    });
  }

  /**
   * {@inheritDoc}
   * 
   * @throws Exception if there is an error during port connection or if smth
   *                   external happens, for example interrupted
   * 
   * @see #sendResult(String, EndPointI, String, String, Serializable)
   * @see URIStamper
   */
  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    // Using computationURI as the key as URIStamper's method is not thread safe. so
    // the first node will store the data in a different uri than the others but it
    // doesn't matter
    mapResults.compute(computationURI, (uri, existingFuture) -> {
      this.logMessage("[NODE-REDUCE] Reducing with URI: " + computationURI + " and accumulator: " + currentAcc);

      if (this.nodeURI.equals(URIStamper.getOriginNodeFromUri(computationURI))) {
        this.logMessage("[NODE-REDUCE] looped Reduce ! Returing acc " + currentAcc);
        try {
          this.sendResult("REDUCE", caller, URIStamper.getOriginalComputationUriFromUri(computationURI), this.nodeURI,
              currentAcc);
        } catch (Exception e) {
          this.logMessage("[NODE-REDUCE] ERROR sending reduce result: " + e.getMessage());
          e.printStackTrace();
        }
        return existingFuture;
      }

      if (existingFuture == null && !URIStamper.isComputationUriStamped(computationURI)) {
        this.logMessage("[NODE-REDUCE] WARNING nothing in the mapResult, maybe received reduce before map ?");
        this.logMessage("[NODE-REDUCE] not stamping the uri and sending to next node after a short delay");
        try {
          Thread.sleep(20);
          this.getNextMapReduceReference().reduce(computationURI, reductor, combinator, identityAcc, currentAcc,
              caller);
        } catch (Exception e) {
          this.logMessage("[NODE-REDUCE] ERROR sending result to next node");
          e.printStackTrace();
        }
      }

      if (existingFuture == null) {
        this.logMessage("[NODE-REDUCE] WARNING smh nothing the mapresult and uri already stamped ? Something is wrong");
        return existingFuture;
      }

      String newUri = URIStamper.isComputationUriStamped(computationURI) ? computationURI
          : URIStamper.stampOriginNodeToUri(computationURI, this.nodeURI);

      existingFuture.thenAcceptAsync(stream -> {
        Stream<R> typedStream = (Stream<R>) stream;
        A result = typedStream.reduce(identityAcc, reductor, combinator);
        A newAcc = combinator.apply(currentAcc, result);
        try {
          this.logMessage("[NODE-REDUCE] Calling next node reduce with URI: " + newUri + " and newAcc: " + newAcc);
          this.getNextMapReduceReference().reduce(newUri, reductor, combinator, identityAcc,
              newAcc, caller);
        } catch (Exception e) {
          this.logMessage("[NODE-REDUCE] ERROR sending reduce to next node : " + e.getMessage());
          e.printStackTrace();
        }
      });
      return existingFuture;
    });

  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  /**
   * Sends a computation result to a caller through a specified endpoint.
   * 
   * @param <I>            The interface type extending ResultReceptionCI
   * @param method         The method name for logging purposes
   * @param caller         The endpoint to which the result will be sent
   * @param computationURI The URI identifying the computation
   * @param result         The result of the computation to be sent
   * 
   * @throws Exception If an error occurs during the connection or smth external
   */
  protected <I extends ResultReceptionCI> void sendResult(String method, EndPointI<I> caller, String computationURI,
      Serializable result) throws Exception {
    this.logMessage("[NODE-" + method + "] Sending result with computation URI: " + computationURI + " and result: "
        + result);
    caller.initialiseClientSide(this);
    caller.getClientSideReference().acceptResult(computationURI, result);
    caller.cleanUpClientSide();
  }

  /**
   * Sends a result from a map-reduce computation to a specified caller.
   * 
   * @param <I>            The interface type extending MapReduceResultReceptionCI
   * @param method         The method name for logging purposes
   * @param caller         The endpoint that will receive the result
   * @param computationURI The URI identifying the computation
   * @param emitterId      The ID of the emitter sending the result
   * @param acc            The accumulated result to be sent (must be
   *                       Serializable)
   * 
   * @throws Exception If an error occurs during the connection or smth external
   */
  protected <I extends MapReduceResultReceptionCI> void sendResult(String method, EndPointI<I> caller,
      String computationURI,
      String emitterId, Serializable acc) throws Exception {
    this.logMessage("[NODE-" + method + "] Sending result with computation URI: " + computationURI + " and result: "
        + acc);
    caller.initialiseClientSide(this);
    caller.getClientSideReference().acceptResult(computationURI, emitterId, acc);
    caller.cleanUpClientSide();
  }

  /**
   * Returns the URI of the executor service used for content access operations.
   *
   * @return the content access executor service URI
   */
  public String getContentAccessExecutorServiceURI() {
    return this.contentAccessExecutorServiceURI;
  }

  /**
   * Returns the URI of the MapReduce executor service.
   * 
   * @return the URI of the MapReduce executor service
   */
  public String getMapReduceExecutorServiceURI() {
    return this.mapReduceExecutorServiceURI;
  }

}
