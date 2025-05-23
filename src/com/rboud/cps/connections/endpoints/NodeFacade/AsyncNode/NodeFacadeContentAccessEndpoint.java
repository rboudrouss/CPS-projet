package com.rboud.cps.connections.endpoints.NodeFacade.AsyncNode;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessOutboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessAsyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

/**
 * Endpoint managing asynchronous content access between nodes and the facade.
 * Creates and manages ports for content operations with asynchronous callbacks.
 */
public class NodeFacadeContentAccessEndpoint extends BCMEndPoint<ContentAccessCI> {

  /**
   * Creates a new endpoint for asynchronous content access with default
   * configuration.
   */
  public NodeFacadeContentAccessEndpoint() {
    super(ContentAccessCI.class, ContentAccessCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public NodeFacadeContentAccessEndpoint(String inboundPortURI) {
    super(ContentAccessCI.class, ContentAccessCI.class, inboundPortURI);
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
    FacadeContentAccessOutboundPort port = new FacadeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
