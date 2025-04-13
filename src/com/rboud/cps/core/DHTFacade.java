package com.rboud.cps.core;

import java.io.Serializable;

import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
@OfferedInterfaces(offered = { DHTServicesCI.class })
public class DHTFacade extends AbstractComponent implements DHTServicesI {
  private NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint;
  private FacadeClientDHTServicesEndpoint facadeClientDHTServicesEndpoint;

  private final static String URI_PREFIX = "dht-facade-";

  protected DHTFacade(NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint,
      FacadeClientDHTServicesEndpoint facadeClientDHTServicesEndpoint)
      throws Exception {
    super(1, 0);
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.facadeClientDHTServicesEndpoint = facadeClientDHTServicesEndpoint;

    assert this.nodeFacadeCompositeEndpoint.serverSideInitialised();
    try {
      this.facadeClientDHTServicesEndpoint.initialiseServerSide(this);
      this.nodeFacadeCompositeEndpoint.initialiseClientSide(this);
    } catch (Exception e) {
      throw new Exception(e);
    }

    this.toggleLogging();
    this.toggleTracing();
  }

  // ------------------------------------------------------------------------
  // Component lifecycle methods
  // ------------------------------------------------------------------------

  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[DHT-FACADE] Starting DHT Facade component.");
    super.start();
  }

  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[DHT-FACADE] Finalising DHT Facade component.");
    this.printExecutionLogOnFile("logs/dht-facade");

    this.facadeClientDHTServicesEndpoint.cleanUpServerSide();
    this.nodeFacadeCompositeEndpoint.cleanUpClientSide();

    super.finalise();
  }


  // ------------------------------------------------------------------------
  // DHTServicesI implementation
  // ------------------------------------------------------------------------

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    this.logMessage("[DHT-FACADE] Getting content with key: " + key);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::getSync,
        key // formatter hack
    );
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    this.logMessage("[DHT-FACADE] Putting content with key: " + key + " and value: " + value);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::putSync,
        key,
        value // formatter hack
    );
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    this.logMessage("[DHT-FACADE] Removing content with key: " + key);
    return this.contentComputeAndClear(
        this.getContentAccessClientReference()::removeSync,
        key // formatter hack
    );
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    this.logMessage("[DHT-FACADE] Starting mapReduce computation.");
    String computeURI = URIGenerator.generateURI(URI_PREFIX);

    this.getMapReduceClientReference().mapSync(computeURI, selector, processor);
    A out = this.getMapReduceClientReference().reduceSync(computeURI, reductor, combinator,
        initialAcc);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return out;
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  private ContentAccessSyncCI getContentAccessClientReference() {
    return this.nodeFacadeCompositeEndpoint.getContentAccessEndpoint().getClientSideReference();
  }

  private <U, R extends ContentDataI> R contentComputeAndClear(ThrowingBiFunction<String, U, R> func, U arg)
      throws Exception {
    String computeURI = URIGenerator.generateURI(URI_PREFIX);
    R result = func.apply(computeURI, arg);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return result;
  }

  private <U, V, R extends ContentDataI> R contentComputeAndClear(ThrowingTriFunction<String, U, V, R> func, U arg1,
      V arg2)
      throws Exception {
    String computeURI = URIGenerator.generateURI(URI_PREFIX);
    R result = func.apply(computeURI, arg1, arg2);
    this.getMapReduceClientReference().clearMapReduceComputation(computeURI);
    return result;
  }

  private MapReduceSyncCI getMapReduceClientReference() {
    return this.nodeFacadeCompositeEndpoint.getMapReduceEndpoint().getClientSideReference();
  }


  @FunctionalInterface
  public interface ThrowingTriFunction<T, U, V, R> {
    R apply(T t, U u, V v) throws Exception;
  }

  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws Exception;
  }
}
