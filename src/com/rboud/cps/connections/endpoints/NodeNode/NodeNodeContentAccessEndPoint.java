package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;
import com.rboud.cps.connections.ports.Node.NodeContentAccessOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class NodeNodeContentAccessEndPoint extends BCMEndPoint<ContentAccessSyncCI> {

  public NodeNodeContentAccessEndPoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  public NodeNodeContentAccessEndPoint(String uri) {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, uri);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return (NodeContentAccessInboundPort) port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessOutboundPort port = new NodeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
