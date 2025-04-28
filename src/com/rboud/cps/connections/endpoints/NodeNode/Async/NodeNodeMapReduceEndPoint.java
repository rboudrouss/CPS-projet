package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * Endpoint managing asynchronous MapReduce operations between two DHT nodes.
 * Creates and manages ports for MapReduce operations between peer nodes.
 */
public class NodeNodeMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

  /**
   * Creates a new endpoint for asynchronous MapReduce operations with default
   * configuration.
   */
  public NodeNodeMapReduceEndPoint() {
    super(MapReduceCI.class, MapReduceCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof AsyncNode;
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c,
        ((AsyncNode) c).getMapReduceExecutorServiceURI());
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceOutboundPort port = new NodeMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceConnector.class.getCanonicalName());
    return port;
  }

}
