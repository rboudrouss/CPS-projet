package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class NodeNodeMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

  public NodeNodeMapReduceEndPoint() {
    super(MapReduceCI.class, MapReduceCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

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
