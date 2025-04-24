package com.rboud.cps.connections.ports.Node;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class NodeMapReduceSyncInboundPort extends AbstractInboundPort implements MapReduceSyncCI {

  public NodeMapReduceSyncInboundPort(ComponentI owner) throws Exception {
    super(MapReduceSyncCI.class, owner);
  }

  public NodeMapReduceSyncInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, MapReduceSyncCI.class, owner);
  }

  @Override
  public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceSyncI) c).mapSync(computationURI, selector, processor);
      return null;
    });
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
      CombinatorI<A> combinator, A currentAcc) throws Exception {
    return this.getOwner()
        .handleRequest(c -> ((MapReduceSyncI) c).reduceSync(computationURI, reductor, combinator, currentAcc));
  }

  @Override
  public void clearMapReduceComputation(String computationURI) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceSyncI) c).clearMapReduceComputation(computationURI);
      return null;
    });
  }
}
