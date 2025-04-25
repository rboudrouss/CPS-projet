package com.rboud.cps.connections.ports.Node.Async;

import java.io.Serializable;

import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncOutboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class NodeMapReduceOutboundPort extends NodeMapReduceSyncOutboundPort implements MapReduceCI {

  public NodeMapReduceOutboundPort(ComponentI owner)
      throws Exception {
    super(MapReduceCI.class, owner);
  }

  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    ((MapReduceCI) this.getConnector()).map(computationURI, selector, processor);
  }

  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    ((MapReduceCI) this.getConnector()).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, caller);
  }

}
