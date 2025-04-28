package com.rboud.cps.connections.ports.Node.Async;

import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncOutboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * Outbound port implementing asynchronous content access for DHT nodes.
 * Extends the synchronous port to handle asynchronous content operations with
 * callbacks.
 */
public class NodeContentAccessAsyncOutboundPort extends NodeContentAccessSyncOutboundPort implements ContentAccessCI {

  /**
   * Creates a new content access outbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessAsyncOutboundPort(ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, owner);
  }

  /**
   * Creates a new content access outbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessAsyncOutboundPort(String URI, ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, URI, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessCI) this.getConnector()).get(computationURI, key, caller);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    ((ContentAccessCI) this.getConnector()).put(computationURI, key, value, caller);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessCI) this.getConnector()).remove(computationURI, key, caller);
  }

}
