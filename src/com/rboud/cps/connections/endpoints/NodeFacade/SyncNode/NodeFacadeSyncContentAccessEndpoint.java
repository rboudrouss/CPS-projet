package com.rboud.cps.connections.endpoints.NodeFacade.SyncNode;

import com.rboud.cps.connections.connectors.ContentAccessSyncConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessSyncOutboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

/**
 * Endpoint managing synchronous content access between nodes and the facade.
 * Creates and manages ports for synchronous content operations between
 * components.
 */
public class NodeFacadeSyncContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  /**
   * Creates a new endpoint for synchronous content access with default
   * configuration.
   */
  public NodeFacadeSyncContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public NodeFacadeSyncContentAccessEndpoint(String inboundPortURI) {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, inboundPortURI);
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
    FacadeContentAccessSyncOutboundPort port = new FacadeContentAccessSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessSyncConnector.class.getCanonicalName());
    return port;
  }

}