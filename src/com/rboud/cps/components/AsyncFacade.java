package com.rboud.cps.components;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class AsyncFacade<CAI extends ContentAccessI, MRI extends MapReduceI> extends SyncFacade<CAI, MRI>
    implements ResultReceptionI, MapReduceResultReceptionI {

  protected ConcurrentHashMap<String, CompletableFuture<Serializable>> computationResults = new ConcurrentHashMap<>();

  protected AsyncFacade(
      ContentNodeBaseCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      EndPointI<DHTServicesI> facadeClientDHTServicesEndpoint) throws Exception {
    super(nodeFacadeCompositeEndpoint, facadeClientDHTServicesEndpoint);
  }

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    this.logMessage("[DHT-FACADE] Getting content with key: " + key);

    this.contentComputeAndClear(
      (computationURI, k, caller) -> {
        this.getContentAccessClientReference().get(computationURI, k, caller);
      },
      key,
      (EndPointI<ResultReceptionI>) null
    
    );
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    this.logMessage("[DHT-FACADE] Getting content with key: " + key + " and URI: " + computationURI);
    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);
    return (ContentDataI) result.get();
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    // TODO Auto-generated method stub
    return super.remove(key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
    // TODO Auto-generated method stub
    return super.mapReduce(selector, processor, reductor, combinator, initialAcc);
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    throw new UnsupportedOperationException("Unimplemented method 'acceptResult'");
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

  @Override
  protected <U, R extends ContentDataI> R contentComputeAndClear(ThrowingBiFunction<String, U, R> func, U arg)
      throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    func.apply(computationURI, arg);

    R out = (R) result.get();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  @Override
  protected <U, V, R extends ContentDataI> R contentComputeAndClear(ThrowingTriFunction<String, U, V, R> func, U arg1,
      V arg2) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    func.apply(computationURI, arg1, arg2);

    R out = (R) result.get();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  protected <U, V, W, R extends ContentDataI> R contentComputeAndClear(ThrowingQuadFunction<String, U, V, W, R> func,
      U arg1, V arg2, W arg3) throws Exception {
    String computationURI = URIGenerator.generateURI(URI_PREFIX);
    CompletableFuture<Serializable> result = new CompletableFuture<>();
    this.computationResults.put(computationURI, result);

    func.apply(computationURI, arg1, arg2, arg3);

    R out = (R) result.get();

    this.getContentAccessClientReference().clearComputation(computationURI);
    return out;
  }

  @FunctionalInterface
  public interface ThrowingQuadFunction<T, U, V, W, R> {
    R apply(T t, U u, V v, W w) throws Exception;
  }

}
