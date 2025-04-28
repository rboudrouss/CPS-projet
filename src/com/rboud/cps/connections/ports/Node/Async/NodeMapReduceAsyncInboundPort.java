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
 * Represents an inbound port for MapReduce operations in a distributed
 * computing environment.
 * This class extends NodeMapReduceSyncInboundPort and implements the
 * MapReduceCI interface
 * to provide asynchronous MapReduce capabilities.
 * 
 * The port handles incoming map and reduce operation requests and delegates
 * them to the
 * component owner for execution. It supports both basic MapReduce operations
 * and
 * parameterized operations with custom interfaces.
 *
 * Map operations are executed asynchronously using the specified executor
 * service,
 * processing data through provided selectors and processors.
 *
 * Reduce operations combine intermediate results using specified reductors and
 * combinators,
 * maintaining state through accumulators and notifying callers of results.
 *
 * @see NodeMapReduceSyncInboundPort
 * @see MapReduceCI
 * @see MapReduceI
 */
public class NodeMapReduceAsyncInboundPort extends NodeMapReduceSyncInboundPort implements MapReduceCI {

  /**
   * Creates a new MapReduce inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceAsyncInboundPort(ComponentI owner)
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
  public NodeMapReduceAsyncInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(MapReduceCI.class, URI, owner);
  }

  /**
   * Creates a new MapReduce inbound port with the specified URI and interface.
   *
   * @param URI                  The unique URI for this port
   * @param owner                The component owner of this port
   * @param implementedInterface The interface implemented by this port
   * @throws Exception If port creation fails
   */
  public <MRI extends MapReduceCI> NodeMapReduceAsyncInboundPort(String URI, ComponentI owner,
      Class<MRI> implementedInterface)
      throws Exception {
    super(implementedInterface, URI, owner);
  }

  /**
   * Creates a new MapReduce inbound port with the specified interface.
   *
   * @param owner                The component owner of this port
   * @param implementedInterface The interface implemented by this port
   * @throws Exception If port creation fails
   */
  public <MRI extends MapReduceCI> NodeMapReduceAsyncInboundPort(ComponentI owner, Class<MRI> implementedInterface)
      throws Exception {
    super(implementedInterface, owner);
  }

  /**
   * Creates a new MapReduce inbound port with the specified interface and
   * executor service URI.
   * 
   * @param owner                The component owner of this port
   * @param implementedInterface The interface implemented by this port
   * @param executorServiceURI   The URI of the executor service for this port
   * 
   */
  public <MRI extends MapReduceCI> NodeMapReduceAsyncInboundPort(ComponentI owner, Class<MRI> implementedInterface,
      String executorServiceURI) throws Exception {
    super(implementedInterface, owner, null, executorServiceURI);
  }

  /**
   * Creates a new MapReduce inbound port with the specified URI, owner,
   * interface,
   * and executor service URI.
   *
   * @param URI                  The unique URI for this port
   * @param owner                The component owner of this port
   * @param implementedInterface The interface implemented by this port
   * @param executorServiceURI   The URI of the executor service for this port
   * @throws Exception If port creation fails
   */
  public <MRI extends MapReduceCI> NodeMapReduceAsyncInboundPort(String URI, ComponentI owner,
      Class<MRI> implementedInterface,
      String executorServiceURI) throws Exception {
    super(URI, implementedInterface, owner, null, executorServiceURI);
  }

  /**
   * Creates a new MapReduce inbound port with specified interface.
   *
   * @param URI                The URI of the port
   * @param owner              The component owner of this port
   * @param executorServiceURI The URI of the executor service for this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceAsyncInboundPort(String URI, ComponentI owner, String executorServiceURI)
      throws Exception {
    super(URI, MapReduceCI.class, owner, null, executorServiceURI);
    assert executorServiceURI != null : "executorServiceURI cannot be null";
  }

  /**
   * Creates a new MapReduce inbound port with specified interface and URI.
   *
   * @param owner              The component owner of this port
   * @param executorServiceURI The URI of the executor service for this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceAsyncInboundPort(ComponentI owner, String executorServiceURI)
      throws Exception {
    super(MapReduceCI.class, owner, null, executorServiceURI);
    assert executorServiceURI != null : "executorServiceURI cannot be null";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        c -> {
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
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        c -> {
          try {
            ((MapReduceI) c).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }
}
