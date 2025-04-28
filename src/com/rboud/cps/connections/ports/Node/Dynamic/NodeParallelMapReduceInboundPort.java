package com.rboud.cps.connections.ports.Node.Dynamic;

import java.io.Serializable;

import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceAsyncInboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class NodeParallelMapReduceInboundPort extends NodeMapReduceAsyncInboundPort implements ParallelMapReduceCI {

  public NodeParallelMapReduceInboundPort(ComponentI owner) throws Exception {
    super(owner);
  }

  public NodeParallelMapReduceInboundPort(String uri, ComponentI owner, String executorServiceURI) throws Exception {
    super(uri, owner, executorServiceURI);
  }

  @Override
  public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
      ParallelismPolicyI parallelismPolicy) throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        (c) -> {
          try {
            ((ParallelMapReduceCI) c).parallelMap(computationURI, selector, processor, parallelismPolicy);
          } catch (Exception e) {
            e.printStackTrace();
          }
          return;
        }

    );
  }

  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
      ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        (c) -> {
          try {
            ((ParallelMapReduceCI) c).parallelReduce(computationURI, reductor, combinator, identityAcc, currentAcc,
                parallelismPolicy, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
          return;
        });
  }

}
