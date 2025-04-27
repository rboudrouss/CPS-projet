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

/**
 * Inbound port implementing DHT services for the facade.
 * Handles incoming requests for content management and MapReduce operations.
 */
public class FacadeDHTServicesInboundPort extends AbstractInboundPort implements DHTServicesCI {

  /**
   * Creates a new DHT services inbound port for the facade.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeDHTServicesInboundPort(ComponentI owner) throws Exception {
    super(DHTServicesCI.class, owner);
  }

  /**
   * Creates a new DHT services inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeDHTServicesInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, DHTServicesCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).get(key));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).put(key, value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((DHTServicesI) c).remove(key));
  }

  /**
   * {@inheritDoc}
   */
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
