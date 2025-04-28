/**
 * A facade component that provides asynchronous access to DHT (Distributed Hash Table) services 
 * and MapReduce operations. This class extends SyncFacade to provide non-blocking operations
 * using CompletableFuture for handling asynchronous results.
 * 
 * <p>The class implements ResultReceptionI and MapReduceResultReceptionI interfaces to handle
 * the reception of results from both content access and MapReduce operations.</p>
 * 
 * @param <CAI> The type parameter extending ContentAccessI interface
 * @param <MRI> The type parameter extending MapReduceI interface
 * 
 * @OfferedInterfaces Offers ResultReceptionCI, MapReduceResultReceptionCI, and DHTServicesCI interfaces
 * @RequiredInterfaces Requires MapReduceCI and ContentAccessCI interfaces
 * 
 * @see SyncFacade
 * @see ResultReceptionI
 * @see MapReduceResultReceptionI
 * @see ContentAccessI
 * @see MapReduceI
 */
package com.rboud.cps.components;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.rboud.cps.connections.endpoints.NodeFacade.FacadeReception.NodeFacadeMapReduceResultReceptionEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.FacadeReception.NodeFacadeResultReceptionEndpoint;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * An asynchronous facade implementation that provides asynchronous access to DHT services
 * and MapReduce operations. This class extends SyncFacade and implements both ResultReceptionI
 * and MapReduceResultReceptionI interfaces to handle asynchronous result callbacks.
 *
 * <p>This facade uses CompletableFuture to manage asynchronous operations and their results.
 * It maintains a concurrent hash map to store computation results associated with their URIs.
 * The connection between the facade and the node is asynchronous, but the conenction between
 * the facade and the client is synchronous. The facade will wait for the result reception.</p>
 *
 * @param <CAI> The type parameter extending ContentAccessI interface
 * @param <MRI> The type parameter extending MapReduceI interface
 *
 * @OfferedInterfaces Offers ResultReceptionCI, MapReduceResultReceptionCI, and DHTServicesCI interfaces
 * @RequiredInterfaces Requires MapReduceCI and ContentAccessCI interfaces
 */
@OfferedInterfaces(offered = { ResultReceptionCI.class, MapReduceResultReceptionCI.class, DHTServicesCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class })
public class AsyncFacade<CAI extends ContentAccessI, MRI extends MapReduceI> extends SyncFacade<CAI, MRI>
    implements ResultReceptionI, MapReduceResultReceptionI {

  /**
   * Endpoint for receiving results from the content access operations.
   */
  EndPointI<ResultReceptionCI> facadeResultReceptionEndpoint;

  /**
   * Endpoint for receiving results from the MapReduce operations.
   */
  EndPointI<MapReduceResultReceptionCI> facadeMapReduceResultReceptionEndpoint;

  /**
   * A map to store the computation results associated with their respective URIs.
   * (to be more correct, just stores the future so it can be shared between the
   * result reception method and the implementation)
   */
  protected ConcurrentHashMap<String, CompletableFuture<Serializable>> computationResults = new ConcurrentHashMap<>();

  protected AsyncFacade(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      EndPointI<DHTServicesI> facadeClientDHTServicesEndpoint) throws Exception {
    super(nodeFacadeCompositeEndpoint, facadeClientDHTServicesEndpoint);

    this.facadeResultReceptionEndpoint = new NodeFacadeResultReceptionEndpoint();
    this.facadeMapReduceResultReceptionEndpoint = new NodeFacadeMapReduceResultReceptionEndpoint();
    this.facadeResultReceptionEndpoint.initialiseServerSide(this);
    this.facadeMapReduceResultReceptionEndpoint.initialiseServerSide(this);
  }

  /**
   * @see DHTServicesI#get(ContentKeyI)
   */
  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[FACADE] Getting content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().get(computationURI, key,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    ContentDataI out = (ContentDataI) result.get();

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;

  }

  /**
   * @see DHTServicesI#put(ContentKeyI, ContentDataI)
   */
  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[FACADE] Putting content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().put(computationURI, key, value,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    this.logMessage("[FACADE] Waiting for reception.");
    ContentDataI out = (ContentDataI) result.get();
    this.logMessage("[FACADE] Result reception endpoint finished.");

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  /**
   * @see DHTServicesI#remove(ContentKeyI)
   */
  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[FACADE] Removing content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().remove(computationURI, key,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    ContentDataI out = (ContentDataI) result.get();

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  /**
   * @see DHTServicesI#mapReduce(SelectorI, ProcessorI, ReductorI, CombinatorI,
   *      Serializable)
   */
  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[FACADE] Starting mapReduce computation with URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);
    this.getMapReduceClientReference().map(computationURI, selector, processor);

    this.getMapReduceClientReference().reduce(computationURI, reductor, combinator, initialAcc, initialAcc,
        this.facadeMapReduceResultReceptionEndpoint.copyWithSharable());

    A out = (A) result.get();
    assert this.facadeMapReduceResultReceptionEndpoint.clientSideClean();
    this.getMapReduceClientReference().clearMapReduceComputation(computationURI);

    this.logMessage("[FACADE] MapReduce computation finished with URI: " + computationURI);
    return out;
  }

  /**
   * @see MapReduceResultReceptionI#acceptResult(String, String, Serializable)
   */
  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    this.logMessage(
        "[FACADE] Accepting Map Reduce result with URI: " + computationURI + " and emitterId: " + emitterId);

    CompletableFuture<Serializable> future = this.computationResults.remove(computationURI);
    if (future != null) {
      future.complete(acc);
    } else {
      throw new Exception("No computation result found for URI: " + computationURI);
    }
  }

  /**
   * @see ResultReceptionI#acceptResult(String, Serializable)
   */
  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    this.logMessage("[FACADE] Accepting Content Access result with URI: " + computationURI);

    CompletableFuture<Serializable> future = this.computationResults.remove(computationURI);
    if (future != null) {
      future.complete(result);
    } else {
      throw new Exception("No computation result found for URI: " + computationURI);
    }
  }

}
