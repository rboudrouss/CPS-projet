package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.ports.Node.NodeResultReceptionInboundPort;
import com.rboud.cps.connections.ports.Node.NodeResultReceptionOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class NodeNodeResultReceptionEndPoint extends BCMEndPoint<ResultReceptionCI> {
  public NodeNodeResultReceptionEndPoint() {
    super(ResultReceptionCI.class, ResultReceptionCI.class);
  }

  public NodeNodeResultReceptionEndPoint(String inboundPortURI) {
    super(ResultReceptionCI.class, ResultReceptionCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeResultReceptionInboundPort port = new NodeResultReceptionInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeResultReceptionOutboundPort port = new NodeResultReceptionOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ResultReceptionCI.class.getCanonicalName());
    return port;
  }

}
