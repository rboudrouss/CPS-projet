package com.rboud.cps.connections.endpoints.NodeFacade.NodeAsync;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceOutboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * Endpoint managing asynchronous MapReduce operations between nodes and the
 * facade.
 * Creates and manages ports for MapReduce operations with asynchronous
 * callbacks.
 */
public class NodeFacadeMapReduceEndpoint extends BCMEndPoint<MapReduceCI> {

  /**
   * Creates a new endpoint for asynchronous MapReduce operations with default
   * configuration.
   */
  public NodeFacadeMapReduceEndpoint() {
    super(MapReduceCI.class, MapReduceCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public NodeFacadeMapReduceEndpoint(String inboundPortURI) {
    super(MapReduceCI.class, MapReduceCI.class, inboundPortURI);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof AsyncNode;
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c, ((AsyncNode) c)
        .getMapReduceExecutorServiceURI());
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceOutboundPort port = new FacadeMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceConnector.class.getCanonicalName());
    return port;
  }
}
