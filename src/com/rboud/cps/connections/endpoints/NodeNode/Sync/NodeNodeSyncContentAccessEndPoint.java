package com.rboud.cps.connections.endpoints.NodeNode.Sync;

import com.rboud.cps.connections.connectors.ContentAccessSyncConnector;
import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncInboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

/**
 * Endpoint managing synchronous content access between two DHT nodes.
 * Creates and manages ports for synchronous content operations between peer
 * nodes.
 */
public class NodeNodeSyncContentAccessEndPoint extends BCMEndPoint<ContentAccessSyncCI> {

  /**
   * Creates a new endpoint for synchronous content access with default
   * configuration.
   */
  public NodeNodeSyncContentAccessEndPoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  /**
   * Creates a new endpoint with a specific URI.
   *
   * @param uri The URI for this endpoint
   */
  public NodeNodeSyncContentAccessEndPoint(String uri) {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, uri);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessSyncInboundPort port = new NodeContentAccessSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessSyncOutboundPort port = new NodeContentAccessSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessSyncConnector.class.getCanonicalName());
    return port;
  }

}
