package com.rboud.cps.connections.ports.Node.Async;

import java.io.Serializable;

import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncInboundPort;

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
 * Inbound port handling asynchronous MapReduce operations for DHT nodes.
 * Extends the synchronous port to handle asynchronous map and reduce
 * operations.
 */
public class NodeMapReduceInboundPort extends NodeMapReduceSyncInboundPort implements MapReduceCI {

  /**
   * Creates a new MapReduce inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceInboundPort(ComponentI owner)
      throws Exception {
    super(MapReduceCI.class, owner);
  }

  /**
   * Creates a new MapReduce inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(MapReduceCI.class, URI, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.getOwner().runTask(c -> {
      try {
        ((MapReduceI) c).map(computationURI, selector, processor);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    this.getOwner().runTask(c -> {
      try {
        ((MapReduceI) c).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, caller);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }
}
