package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.PortI;
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
    PortI port = this.getPortFromURI(c, inboundPortURI);
    if (port != null) {
      return (NodeContentAccessInboundPort) port;
    }
    port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return (NodeContentAccessInboundPort) port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'makeOutboundPort'");
  }



}
