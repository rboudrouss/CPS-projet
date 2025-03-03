package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class FacadeDHTServicesInboundPort extends AbstractInboundPort implements DHTServicesCI {

  public FacadeDHTServicesInboundPort(ComponentI owner) throws Exception {
    super(DHTServicesCI.class, owner);
  }

  public FacadeDHTServicesInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, DHTServicesCI.class, owner);
  }

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).get(key));
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).put(key, value));
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).remove(key));
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).mapReduce(selector, processor, reductor, combinator,
        initialAcc));
  }

}
