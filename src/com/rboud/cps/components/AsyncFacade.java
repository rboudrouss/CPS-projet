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

@OfferedInterfaces(offered = { ResultReceptionCI.class, MapReduceResultReceptionCI.class, DHTServicesCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class })
public class AsyncFacade<CAI extends ContentAccessI, MRI extends MapReduceI> extends SyncFacade<CAI, MRI>
    implements ResultReceptionI, MapReduceResultReceptionI {

  // please note that we are obliged to use the CI interface cause of the common
  // apis. the content access methode defines EndpointI<I> with I extending
  // ResultReceptionCI. This may make it hard to swap the implementation to a
  // different non-BCM Endpoint.
  EndPointI<ResultReceptionCI> facadeResultReceptionEndpoint;
  EndPointI<MapReduceResultReceptionCI> facadeMapReduceResultReceptionEndpoint;

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

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[DHT-FACADE] Getting content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().get(computationURI, key,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    ContentDataI out = (ContentDataI) result.get();

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;

  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[DHT-FACADE] Putting content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().put(computationURI, key, value,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    ContentDataI out = (ContentDataI) result.get();

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[DHT-FACADE] Removing content with key: " + key + " and URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    this.getContentAccessClientReference().remove(computationURI, key,
        this.facadeResultReceptionEndpoint.copyWithSharable());
    ContentDataI out = (ContentDataI) result.get();

    assert this.facadeResultReceptionEndpoint.clientSideClean();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[DHT-FACADE] Starting mapReduce computation with URI: " + computationURI);

    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);
    this.getMapReduceClientReference().map(computationURI, selector, processor);

    this.getMapReduceClientReference().reduce(computationURI, reductor, combinator, initialAcc, initialAcc,
        this.facadeMapReduceResultReceptionEndpoint.copyWithSharable());

    A out = (A) result.get();
    assert this.facadeMapReduceResultReceptionEndpoint.clientSideClean();
    this.getMapReduceClientReference().clearMapReduceComputation(computationURI);

    return out;
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    CompletableFuture<Serializable> future = this.computationResults.remove(computationURI);
    if (future != null) {
      future.complete(acc);
    } else {
      throw new Exception("No computation result found for URI: " + computationURI);
    }
  }

  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    CompletableFuture<Serializable> future = this.computationResults.remove(computationURI);
    if (future != null) {
      future.complete(result);
    } else {
      throw new Exception("No computation result found for URI: " + computationURI);
    }
  }

}
