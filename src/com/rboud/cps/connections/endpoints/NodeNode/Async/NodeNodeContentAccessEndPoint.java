package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

/**
 * Endpoint managing asynchronous content access between two DHT nodes.
 * Creates and manages ports for asynchronous content operations between peer
 * nodes.
 */
public class NodeNodeContentAccessEndPoint extends BCMEndPoint<ContentAccessCI> {

  /**
   * Creates a new endpoint for asynchronous content access with default
   * configuration.
   */
  public NodeNodeContentAccessEndPoint() {
    super(ContentAccessCI.class, ContentAccessCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof AsyncNode;
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c,
        ((AsyncNode) c).getContentAccessExecutorServiceURI());
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessOutboundPort port = new NodeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
