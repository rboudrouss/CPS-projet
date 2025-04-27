package com.rboud.cps.connections.connectors;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Connector implementing asynchronous MapReduce operations between components.
 * Extends the synchronous connector to add asynchronous operations with result callbacks.
 */
public class MapReduceConnector extends MapReduceSyncConnector implements MapReduceCI {

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    ((MapReduceCI) this.offering).map(computationURI, selector, processor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    ((MapReduceCI) this.offering).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, caller);
  }

}
