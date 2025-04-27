package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Outbound port implementing asynchronous MapReduce operations for the facade.
 * Extends the synchronous port to add asynchronous operations with result callbacks.
 */
public class FacadeMapReduceOutboundPort extends FacadeMapReduceSyncOutboundPort implements MapReduceCI {

  /**
   * Creates a new MapReduce outbound port for the facade.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeMapReduceOutboundPort(ComponentI owner)
      throws Exception {
    super(MapReduceCI.class, owner);
  }

  /**
   * Creates a new MapReduce outbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeMapReduceOutboundPort(String URI, ComponentI owner)
      throws Exception {
    super(URI, MapReduceCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    ((MapReduceI) this.getConnector()).map(computationURI, selector, processor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    ((MapReduceI) this.getConnector()).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, caller);
  }

}
