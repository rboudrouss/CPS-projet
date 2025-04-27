package com.rboud.cps.connections.endpoints.NodeFacade.FacadeReception;

import com.rboud.cps.connections.connectors.MapReduceResultReceptionConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceResultReceptionInboundPort;
import com.rboud.cps.connections.ports.Node.ResultReception.NodeMapReduceResultReceptionOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

/**
 * Endpoint managing MapReduce result reception between nodes and the facade.
 * Creates and manages ports for receiving computation results from MapReduce
 * operations.
 */
public class NodeFacadeMapReduceResultReceptionEndpoint extends BCMEndPoint<MapReduceResultReceptionCI> {

  /**
   * Creates a new endpoint for MapReduce result reception with default
   * configuration.
   */
  public NodeFacadeMapReduceResultReceptionEndpoint() {
    super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceResultReceptionInboundPort p = new FacadeMapReduceResultReceptionInboundPort(inboundPortURI, c);
    p.publishPort();
    return p;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceResultReceptionOutboundPort p = new NodeMapReduceResultReceptionOutboundPort(c);
    p.publishPort();
    c.doPortConnection(
        p.getPortURI(),
        inboundPortURI,
        MapReduceResultReceptionConnector.class.getCanonicalName());
    return p;
  }

}
