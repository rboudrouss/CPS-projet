package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.connectors.NodeNode.NodeNodeContentAccessConnector;
import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;
import com.rboud.cps.connections.ports.Node.NodeContentAccessOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class NodeNodeContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  public NodeNodeContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  public NodeNodeContentAccessEndpoint(String inboundPortURI) {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessOutboundPort port = new NodeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        NodeNodeContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
