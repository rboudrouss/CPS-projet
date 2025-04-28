package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.connections.connectors.MapReduceAsyncConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceAsyncInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceAsyncOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * Endpoint managing asynchronous MapReduce operations between two DHT nodes.
 * Creates and manages ports for MapReduce operations between peer nodes.
 */
public class NodeNodeAsyncMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

  /**
   * Creates a new endpoint for asynchronous MapReduce operations with default
   * configuration.
   */
  public NodeNodeAsyncMapReduceEndPoint() {
    super(MapReduceCI.class, MapReduceCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof AsyncNode;
    NodeMapReduceAsyncInboundPort port = new NodeMapReduceAsyncInboundPort(inboundPortURI, c,
        ((AsyncNode) c).getMapReduceExecutorServiceURI());
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceAsyncOutboundPort port = new NodeMapReduceAsyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceAsyncConnector.class.getCanonicalName());
    return port;
  }

}
