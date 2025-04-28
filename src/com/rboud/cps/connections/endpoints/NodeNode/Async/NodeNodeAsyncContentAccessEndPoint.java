package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessAsyncInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessAsyncOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

/**
 * Endpoint managing asynchronous content access between two DHT nodes.
 * Creates and manages ports for asynchronous content operations between peer
 * nodes.
 */
public class NodeNodeAsyncContentAccessEndPoint extends BCMEndPoint<ContentAccessCI> {

  /**
   * Creates a new endpoint for asynchronous content access with default
   * configuration.
   */
  public NodeNodeAsyncContentAccessEndPoint() {
    super(ContentAccessCI.class, ContentAccessCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof AsyncNode;
    NodeContentAccessAsyncInboundPort port = new NodeContentAccessAsyncInboundPort(inboundPortURI, c,
        ((AsyncNode) c).getContentAccessExecutorServiceURI());
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessAsyncOutboundPort port = new NodeContentAccessAsyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
