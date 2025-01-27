package com.rboud.cps.core;

import java.io.Serializable;
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

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    return null;
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    return null;
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    return null;
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc
    ) throws Exception {
    return null;
  }

}
