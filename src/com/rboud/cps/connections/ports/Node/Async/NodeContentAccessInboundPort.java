package com.rboud.cps.connections.ports.Node.Async;

import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncInboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * Inbound port handling asynchronous content access for DHT nodes.
 * Extends the synchronous port to handle asynchronous content operations with
 * callbacks.
 */
public class NodeContentAccessInboundPort extends NodeContentAccessSyncInboundPort implements ContentAccessCI {

  /**
   * Creates a new content access inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessInboundPort(ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, owner);
  }

  /**
   * Creates a new content access inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, URI, owner);
  }

  /**
   * Creates a new content access inbound port with specified interface.
   *
   * @param owner              The component owner of this port
   * @param executorServiceURI The URI of the executor service for this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessInboundPort(ComponentI owner, String executorServiceURI)
      throws Exception {
    super(ContentAccessCI.class, owner, null, executorServiceURI);
    assert executorServiceURI != null : "executorServiceURI cannot be null";
  }

  /**
   * Creates a new content access inbound port with specified interface and URI.
   *
   * @param URI                The unique URI for this port
   * @param owner              The component owner of this port
   * @param executorServiceURI The URI of the executor service for this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessInboundPort(String URI, ComponentI owner, String executorServiceURI)
      throws Exception {
    super(URI, ContentAccessCI.class, owner, null, executorServiceURI);
    assert executorServiceURI != null : "executorServiceURI cannot be null";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        (c) -> {
          try {
            ((ContentAccessI) c).get(computationURI, key, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        (c) -> {
          try {
            ((ContentAccessI) c).put(computationURI, key, value, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.getOwner().runTask(
        this.getExecutorServiceURI(),
        (c) -> {
          try {
            ((ContentAccessI) c).remove(computationURI, key, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

}
