package com.rboud.cps.connections.endpoints.NodeFacade.FacadeReception;

import com.rboud.cps.connections.connectors.ResultReceptionConnector;
import com.rboud.cps.connections.ports.Facade.FacadeResultReceptionInboundPort;
import com.rboud.cps.connections.ports.Node.ResultReception.NodeResultReceptionOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * Endpoint managing result reception between nodes and the facade.
 * Creates and manages ports for receiving computation results from content access operations.
 */
public class NodeFacadeResultReceptionEndpoint extends BCMEndPoint<ResultReceptionCI> {

  /**
   * Creates a new endpoint for result reception with default configuration.
   */
  public NodeFacadeResultReceptionEndpoint() {
    super(ResultReceptionCI.class, ResultReceptionCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public NodeFacadeResultReceptionEndpoint(String inboundPortURI) {
    super(ResultReceptionCI.class, ResultReceptionCI.class, inboundPortURI);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeResultReceptionInboundPort p = new FacadeResultReceptionInboundPort(inboundPortURI, c);
    p.publishPort();
    return p;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeResultReceptionOutboundPort p = new NodeResultReceptionOutboundPort(c);
    p.publishPort();
    c.doPortConnection(
        p.getPortURI(),
        inboundPortURI,
        ResultReceptionConnector.class.getCanonicalName());
    return p;
  }

}
