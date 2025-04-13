package com.rboud.cps.connections.endpoints.NodeFacade;

import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessOutboundPort;
import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class NodeFacadeContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  public NodeFacadeContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  public NodeFacadeContentAccessEndpoint(String inboundPortURI) {
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
    FacadeContentAccessOutboundPort port = new FacadeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}