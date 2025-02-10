package com.rboud.cps.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTFacade implements DHTServicesI {
  private List<DHTNode> nodes;

  final String URI = "DHTFacade";

  public DHTFacade(List<DHTNode> nodes) {
    this.nodes = nodes;
  }

  public DHTFacade() {
    this(new ArrayList<>());
  }

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    if (nodes.isEmpty())
      return null;

    return nodes.get(0).getSync(null, key);
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    if (nodes.isEmpty())
      return null;
    return nodes.get(0).putSync(URI, key, value);
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    if (nodes.isEmpty())
      return null;
    return nodes.get(0).removeSync(null, key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    if (nodes.isEmpty())
      return null;

    return nodes.stream().map((node) -> {
      try {
        node.mapSync(URI, selector, processor);
        return node.reduceSync(URI, reductor, combinator, initialAcc);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }).reduce(initialAcc, combinator::apply);
  }

}
