package com.rboud.cps.components;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
    MapReduceResultReceptionCI.class })
public class AsyncNode<CAI extends ContentAccessCI, MRI extends MapReduceCI> extends SyncNode<CAI, MRI>
    implements MapReduceI, ContentAccessI {

  protected ConcurrentHashMap<String, CompletableFuture<Stream<?>>> mapResults = new ConcurrentHashMap<>();

  protected AsyncNode(ContentNodeAsyncCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeAsyncCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeAsyncCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint)
      throws Exception {
    super(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
  }

  protected AsyncNode(ContentNodeAsyncCompositeEndPointI<CAI, MRI> nodeFacadeCompositeEndpoint,
      ContentNodeAsyncCompositeEndPointI<CAI, MRI> selfNodeCompositeEndpoint,
      ContentNodeAsyncCompositeEndPointI<CAI, MRI> nextNodeCompositeEndpoint, int minValue, int maxValue)
      throws Exception {
    super(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint, minValue, maxValue);
  }

  // ------------------------------------------------------------------------
  // Content Access methods
  // ------------------------------------------------------------------------

  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.logMessage("[NODE] Getting content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().get(computationURI, key, caller);
      return;
    }

    this.sendResult(caller, computationURI, this.localStorage.get(key));
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    this.logMessage("[NODE] Putting content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().put(computationURI, key, value, caller);
      return;
    }
    this.sendResult(caller, computationURI, this.localStorage.put(key, value));
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.logMessage("[NODE] Removing content with key: " + key + " and URI: " + computationURI);
    if (!this.interval.in(key.hashCode())) {
      this.getNextContentAccessReference().remove(computationURI, key, caller);
      return;
    }

    this.sendResult(caller, computationURI, this.localStorage.remove(key));
  }

  // ------------------------------------------------------------------------
  // MapReduce methods
  // ------------------------------------------------------------------------

  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.logMessage("[NODE] Mapping with URI: " + computationURI);
    if (this.mapResults.containsKey(computationURI)) {
      this.logMessage("INFO MAP loop detected With URI " + computationURI);
      return;
    }
    mapResults.compute(computationURI, (uri, existingFuture) -> {
      return CompletableFuture.supplyAsync(() -> this.localStorage.values().stream().parallel()
          .filter(selector)
          .map(processor));
    });
  }

  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    this.logMessage("[NODE] Reducing with URI: " + computationURI);

    CompletableFuture<Stream<?>> futureStream = mapResults.get(computationURI);

    if (futureStream == null) {
      this.logMessage("INFO REDUCE loop detected With URI " + computationURI);
      this.sendResult(caller, computationURI, this.nodeURI, currentAcc);
      return;
    }

    futureStream.thenAcceptAsync(stream -> {
      Stream<R> typedStream = (Stream<R>) stream;
      A result = typedStream.reduce(identityAcc, reductor, combinator);
      try {
        this.getNextMapReduceReference().reduce(computationURI, reductor, combinator, identityAcc,
            combinator.apply(currentAcc, result), caller);
      } catch (Exception e) {
        this.logMessage("ERROR returning reduce result: " + e.getMessage());
        e.printStackTrace();
      }
    });

  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  protected <I extends ResultReceptionCI> void sendResult(EndPointI<I> caller, String computationURI,
      Serializable result) throws Exception {
    this.logMessage("[NODE] Sending result to " + caller + " with computation URI: " + computationURI + " and result: "
        + result);
    caller.initialiseClientSide(this);
    caller.getClientSideReference().acceptResult(computationURI, result);
    caller.cleanUpClientSide();
  }

  protected <I extends MapReduceResultReceptionCI> void sendResult(EndPointI<I> caller, String computationURI,
      String emitterId, Serializable acc) throws Exception {
    this.logMessage("[NODE] Sending result to " + caller + " with computation URI: " + computationURI + " and result: "
        + acc);
    caller.initialiseClientSide(this);
    caller.getClientSideReference().acceptResult(computationURI, emitterId, acc);
    caller.cleanUpClientSide();
  }
}
