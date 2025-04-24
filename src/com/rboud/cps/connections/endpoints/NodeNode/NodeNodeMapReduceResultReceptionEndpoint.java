package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.ports.Node.NodeMapReduceResultReceptionInboundPort;
import com.rboud.cps.connections.ports.Node.NodeMapReduceResultReceptionOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class NodeNodeMapReduceResultReceptionEndpoint extends BCMEndPoint<MapReduceResultReceptionCI> {

  public NodeNodeMapReduceResultReceptionEndpoint() {
    super(MapReduceResultReceptionCI.class,MapReduceResultReceptionCI.class);
  }

  public NodeNodeMapReduceResultReceptionEndpoint(String inboundPortURI) {
    super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceResultReceptionInboundPort port = new NodeMapReduceResultReceptionInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceResultReceptionOutboundPort port = new NodeMapReduceResultReceptionOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceResultReceptionCI.class.getCanonicalName());
    return port;
  }

}
