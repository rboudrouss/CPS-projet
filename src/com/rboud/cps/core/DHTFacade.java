package com.rboud.cps.core;

import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class DHTFacade implements DHTServicesI {
  private DHTPOJOEndpoint endpoint;

  static final String URIPrefix = "DHTFacade";
  final String URI;

  public DHTFacade(DHTPOJOEndpoint endpoint) {
    this.endpoint = endpoint;
    this.URI = URIGenerator.generateURI(URIPrefix);
  }

  /*
   * public void printChainNode() {
   * this.node.printChain(URI);
   * }
   */

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    if (!this.endpoint.complete()) {
      throw new Exception("Endpoint not initialized");
    }
    return this.endpoint.getContentAccessEndpoint().getClientSideReference().getSync(URI, key);
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    if (!this.endpoint.complete()) {
      throw new Exception("Endpoint not initialized");
    }
    return this.endpoint.getContentAccessEndpoint().getClientSideReference().putSync(URI, key, value);
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    if (!this.endpoint.complete()) {
      throw new Exception("Endpoint not initialized");
    }
    return this.endpoint.getContentAccessEndpoint().getClientSideReference().removeSync(URI, key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    if (!this.endpoint.complete()) {
      throw new Exception("Endpoint not initialized");
    }

    this.endpoint.getMapReduceEndpoint().getClientSideReference().mapSync(URI, selector, processor);
    return this.endpoint.getMapReduceEndpoint().getClientSideReference().reduceSync(URI, reductor, combinator,
        initialAcc);
  }
}
