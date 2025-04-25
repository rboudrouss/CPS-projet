package com.rboud.cps.connections.endpoints.NodeNode.Async;

import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessInboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

public class NodeNodeContentAccessEndPoint extends BCMEndPoint<ContentAccessCI> {

  public NodeNodeContentAccessEndPoint() {
    super(ContentAccessCI.class, ContentAccessCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessOutboundPort port = new NodeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }
  
}
