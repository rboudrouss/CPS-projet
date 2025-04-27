package com.rboud.cps.components;

import java.io.Serializable;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * A synchronous facade component that implements the DHT services interface.
 * This component acts as an intermediary between clients and the DHT node,
 * providing synchronous access to content management and MapReduce operations.
 *
 * @param <CAI> The type of content access interface
 * @param <MRI> The type of MapReduce interface
 */
@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
@OfferedInterfaces(offered = { DHTServicesCI.class })
public class SyncFacade<CAI extends ContentAccessSyncI, MRI extends MapReduceSyncI> extends AbstractComponent
    implements DHTServicesI {

  /** Prefix used for generating URIs for DHT facade operations */
  protected final static String URI_PREFIX = "dht-facade-";

  /** Composite endpoint for node facade operations */
  private ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint;

  /** Endpoint for client DHT services */
  private EndPointI<DHTServicesI> facadeClientDHTServicesEndpoint;

  /**
   * Creates a new SyncFacade component.
   *
   * @param nodeFacadeCompositeEndpoint     The composite endpoint for node
   *                                        operations
   * @param facadeClientDHTServicesEndpoint The endpoint for client DHT services
   * @throws Exception if component initialization fails
   */
  protected SyncFacade(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      EndPointI<DHTServicesI> facadeClientDHTServicesEndpoint)
      throws Exception {
    super(2, 0);
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.facadeClientDHTServicesEndpoint = facadeClientDHTServicesEndpoint;

    this.facadeClientDHTServicesEndpoint.initialiseServerSide(this);

    this.toggleLogging();
    this.toggleTracing();
    this.getTracer().setTitle("Facade");
  }

  /**
   * Starts the facade component by initializing the client side of the node
   * facade.
   *
   * @throws ComponentStartException if the component fails to start
   */
  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[FACADE] Starting DHT Facade component.");
    try {
      this.nodeFacadeCompositeEndpoint.initialiseClientSide(this);
    } catch (Exception e) {
      throw new ComponentStartException(e);
    }
    super.start();
  }

  /**
   * Finalizes the component by cleaning up resources and connections.
   *
   * @throws Exception if cleanup operations fail
   */
  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[FACADE] Finalising DHT Facade component.");
    this.printExecutionLogOnFile("logs/dht-facade");

    this.facadeClientDHTServicesEndpoint.cleanUpServerSide();
    this.nodeFacadeCompositeEndpoint.cleanUpClientSide();

    super.finalise();
  }

  /**
   * Retrieves content from the DHT using the specified key.
   *
   * @param key The key to lookup
   * @return The content data associated with the key
   * @throws Exception if the operation fails
   */
  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    this.logMessage("[FACADE] Getting content with key: " + key);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::getSync,
        key // formatter hack
    );
  }

  /**
   * Stores content in the DHT with the specified key.
   *
   * @param key   The key under which to store the content
   * @param value The content data to store
   * @return The previous content data if it existed
   * @throws Exception if the operation fails
   */
  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    this.logMessage("[FACADE] Putting content with key: " + key + " and value: " + value);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::putSync,
        key,
        value // formatter hack
    );
  }

  /**
   * Removes content from the DHT with the specified key.
   *
   * @param key The key of the content to remove
   * @return The removed content data
   * @throws Exception if the operation fails
   */
  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    this.logMessage("[FACADE] Removing content with key: " + key);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::removeSync,
        key // formatter hack
    );
  }

  /**
   * Executes a MapReduce computation on the DHT content.
   *
   * @param <R>        The type of intermediate results
   * @param <A>        The type of the final result
   * @param selector   The selector for filtering content
   * @param processor  The processor for mapping content
   * @param reductor   The reductor for reducing results
   * @param combinator The combinator for combining partial results
   * @param initialAcc The initial accumulator value
   * @return The final result of the MapReduce computation
   * @throws Exception if the computation fails
   */
  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    this.logMessage("[FACADE] Starting mapReduce computation.");
    String computeURI = URIGenerator.generateURI(URI_PREFIX);

    this.getMapReduceClientReference().mapSync(computeURI, selector, processor);
    A out = this.getMapReduceClientReference().reduceSync(computeURI, reductor, combinator,
        initialAcc);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return out;
  }

  /**
   * Gets the client reference for content access operations.
   *
   * @return The content access client reference
   */
  protected CAI getContentAccessClientReference() {
    return this.nodeFacadeCompositeEndpoint.getContentAccessEndpoint().getClientSideReference();
  }

  /**
   * Executes a content operation and cleans up the computation URI.
   *
   * @param <U>  The type of the first argument
   * @param <R>  The type of the result
   * @param func The function to execute
   * @param arg  The argument to pass to the function
   * @return The result of the computation
   * @throws Exception if the operation fails
   */
  protected <U, R> R contentComputeAndClear(ThrowingBiFunction<String, U, R> func, U arg)
      throws Exception {
    String computeURI = URIGenerator.generateURI(URI_PREFIX);
    R result = func.apply(computeURI, arg);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return result;
  }

  /**
   * Executes a content operation with two arguments and cleans up the computation
   * URI.
   *
   * @param <U>  The type of the first argument
   * @param <V>  The type of the second argument
   * @param <R>  The type of the result
   * @param func The function to execute
   * @param arg1 The first argument
   * @param arg2 The second argument
   * @return The result of the computation
   * @throws Exception if the operation fails
   */
  protected <U, V, R> R contentComputeAndClear(ThrowingTriFunction<String, U, V, R> func, U arg1,
      V arg2)
      throws Exception {
    String computeURI = URIGenerator.generateURI(URI_PREFIX);
    R result = func.apply(computeURI, arg1, arg2);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return result;
  }

  /**
   * Gets the client reference for MapReduce operations.
   *
   * @return The MapReduce client reference
   */
  protected MRI getMapReduceClientReference() {
    return this.nodeFacadeCompositeEndpoint.getMapReduceEndpoint().getClientSideReference();
  }

  /**
   * Functional interface for operations that take three arguments and may throw
   * an exception.
   *
   * @param <T> The type of the first argument
   * @param <U> The type of the second argument
   * @param <V> The type of the third argument
   * @param <R> The type of the result
   */
  @FunctionalInterface
  public interface ThrowingTriFunction<T, U, V, R> {
    R apply(T t, U u, V v) throws Exception;
  }

  /**
   * Functional interface for operations that take two arguments and may throw an
   * exception.
   *
   * @param <T> The type of the first argument
   * @param <U> The type of the second argument
   * @param <R> The type of the result
   */
  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws Exception;
  }
}
