package com.rboud.cps.connections.ports.Node.Sync;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Inbound port handling synchronous MapReduce operations for DHT nodes.
 * Processes incoming requests for synchronous map, reduce and computation
 * cleanup operations.
 */
public class NodeMapReduceSyncInboundPort extends AbstractInboundPort implements MapReduceSyncCI {

  /**
   * Creates a new synchronous MapReduce inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncInboundPort(ComponentI owner) throws Exception {
    super(MapReduceSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous MapReduce inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, MapReduceSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous MapReduce inbound port with specified interface.
   *
   * @param implementedInterface The interface implemented by this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncInboundPort(Class<? extends MapReduceSyncCI> implementedInterface,
      ComponentI owner) throws Exception {
    super(implementedInterface, owner);
  }

  /**
   * Creates a new synchronous MapReduce inbound port with specified interface and
   * URI.
   *
   * @param implementedInterface The interface implemented by this port
   * @param URI                  The unique URI for this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncInboundPort(Class<? extends MapReduceSyncCI> implementedInterface, String URI,
      ComponentI owner) throws Exception {
    super(URI, implementedInterface, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceSyncI) c).mapSync(computationURI, selector, processor);
      return null;
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
      CombinatorI<A> combinator, A currentAcc) throws Exception {
    return this.getOwner()
        .handleRequest(c -> ((MapReduceSyncI) c).reduceSync(computationURI, reductor, combinator, currentAcc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearMapReduceComputation(String computationURI) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceSyncI) c).clearMapReduceComputation(computationURI);
      return null;
    });
  }
}
