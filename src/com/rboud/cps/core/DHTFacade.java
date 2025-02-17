package com.rboud.cps.core;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class DHTFacade implements DHTServicesI {
  private EndPointI<MapReduceSyncI> mapReduceEndpoint;
  private EndPointI<ContentAccessSyncI> contentAccessEndpoint;

  static final String URIPrefix = "DHTFacade";
  final String URI;

  public DHTFacade(EndPointI<ContentAccessSyncI> contentAccessEndpoint,
      EndPointI<MapReduceSyncI> mapReduceEndpoint) {
    this.mapReduceEndpoint = mapReduceEndpoint;
    this.contentAccessEndpoint = contentAccessEndpoint;
    this.URI = URIGenerator.generateURI(URIPrefix);
  }

  public DHTFacade(DHTPOJOEndpoint compositeEndpoint) {
    this(compositeEndpoint.getContentAccessEndpoint(), compositeEndpoint.getMapReduceEndpoint());
  }

  /*
   * public void printChainNode() {
   * this.node.printChain(URI);
   * }
   */

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    assert this.contentAccessEndpoint.serverSideInitialised();

    return this.contentAccessEndpoint.getClientSideReference().getSync(URI, key);
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    assert this.contentAccessEndpoint.serverSideInitialised();

    return this.contentAccessEndpoint.getClientSideReference().putSync(URI, key, value);
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    assert this.contentAccessEndpoint.serverSideInitialised();

    return this.contentAccessEndpoint.getClientSideReference().removeSync(URI, key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    assert this.mapReduceEndpoint.serverSideInitialised();

    this.mapReduceEndpoint.getClientSideReference().mapSync(URI, selector, processor);
    return this.mapReduceEndpoint.getClientSideReference().reduceSync(URI, reductor, combinator,
        initialAcc);
  }
}
