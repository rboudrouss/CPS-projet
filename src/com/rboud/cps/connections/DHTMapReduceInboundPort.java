package com.rboud.cps.connections;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTMapReduceInboundPort extends AbstractInboundPort implements MapReduceSyncCI {

  public DHTMapReduceInboundPort(ComponentI owner) throws Exception {
    super(MapReduceSyncCI.class, owner);
  }

  public DHTMapReduceInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, MapReduceSyncCI.class, owner);
  }

  @Override
  public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) {
    try {
      this.getOwner().handleRequest(c -> {
        ((MapReduceSyncCI) c).mapSync(computationURI, selector, processor);
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
      CombinatorI<A> combinator, A currentAcc) throws Exception {
    try {
      return this.getOwner()
          .handleRequest(c -> ((MapReduceSyncCI) c).reduceSync(computationURI, reductor, combinator, currentAcc));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clearMapReduceComputation(String computationURI) throws Exception {
    try {
      this.getOwner().handleRequest(c -> {
        ((MapReduceSyncCI) c).clearMapReduceComputation(computationURI);
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
