package com.rboud.cps.connections.connectors;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Connector implementing synchronous MapReduce operations between components.
 * Provides methods for synchronous map, reduce and computation cleanup
 * operations.
 */
public class MapReduceSyncConnector extends AbstractConnector implements MapReduceSyncCI {

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    ((MapReduceSyncCI) this.offering).mapSync(computationURI, selector, processor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
      CombinatorI<A> combinator, A currentAcc) throws Exception {
    return ((MapReduceSyncCI) this.offering).reduceSync(computationURI, reductor, combinator, currentAcc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearMapReduceComputation(String computationURI) throws Exception {
    ((MapReduceSyncCI) this.offering).clearMapReduceComputation(computationURI);
  }

}
