package com.rboud.cps.connections.endpoints.NodeNode.Sync;

import com.rboud.cps.connections.connectors.MapReduceSyncConnector;
import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncInboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

/**
 * Endpoint managing synchronous MapReduce operations between two DHT nodes.
 * Creates and manages ports for synchronous MapReduce operations between peer
 * nodes.
 */
public class NodeNodeSyncMapReduceEndPoint extends BCMEndPoint<MapReduceSyncCI> {

  /**
   * Creates a new endpoint for synchronous MapReduce operations with default
   * configuration.
   */
  public NodeNodeSyncMapReduceEndPoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceSyncInboundPort port = new NodeMapReduceSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceSyncOutboundPort port = new NodeMapReduceSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceSyncConnector.class.getCanonicalName());
    return port;
  }

}
