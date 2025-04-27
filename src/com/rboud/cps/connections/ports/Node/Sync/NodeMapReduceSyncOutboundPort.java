package com.rboud.cps.connections.ports.Node.Sync;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Outbound port implementing synchronous MapReduce operations for DHT nodes.
 * Provides methods for synchronous map, reduce and computation cleanup operations.
 */
public class NodeMapReduceSyncOutboundPort extends AbstractOutboundPort implements MapReduceSyncCI {

  /**
   * Creates a new synchronous MapReduce outbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncOutboundPort(ComponentI owner) throws Exception {
    super(MapReduceSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous MapReduce outbound port with the specified URI.
   *
   * @param uri   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncOutboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, MapReduceSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous MapReduce outbound port with specified interface.
   *
   * @param implementedInterface The interface implemented by this port
   * @param owner              The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncOutboundPort(Class<? extends MapReduceSyncCI> implementedInterface, ComponentI owner)
      throws Exception {
    super(implementedInterface, owner);
  }

  /**
   * Creates a new synchronous MapReduce outbound port with specified interface and URI.
   *
   * @param implementedInterface The interface implemented by this port
   * @param URI                The unique URI for this port
   * @param owner              The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceSyncOutboundPort(Class<? extends MapReduceSyncCI> implementedInterface, String URI,
      ComponentI owner) throws Exception {
    super(URI, implementedInterface, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    ((MapReduceSyncCI) this.getConnector()).mapSync(computationURI, selector, processor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
      CombinatorI<A> combinator, A currentAcc) throws Exception {
    return ((MapReduceSyncCI) this.getConnector()).reduceSync(computationURI, reductor, combinator, currentAcc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearMapReduceComputation(String computationURI) throws Exception {
    ((MapReduceSyncCI) this.getConnector()).clearMapReduceComputation(computationURI);
  }

}
