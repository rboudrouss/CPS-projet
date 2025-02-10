package com.rboud.cps.core;

import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTFacade implements DHTServicesI {
  private DHTNode node;

  final String URI = "DHTFacade";

  public DHTFacade(DHTNode node) {
    this.node = node;
  }

  public DHTFacade() {
    this.node = new DHTNode();
  }
  
  public void printChainNode() {
    this.node.printChain(URI);
  }

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    return this.node.getSync(URI, key);
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    return this.node.putSync(URI, key, value);
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    return this.node.removeSync(URI, key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    this.node.mapSync(URI, selector, processor);
    return this.node.reduceSync(URI, reductor, combinator, initialAcc);
  }
}
