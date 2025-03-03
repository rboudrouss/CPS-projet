package com.rboud.cps.connections.endpoints.NodeFacade;

import com.rboud.cps.connections.connectors.NodeFacade.NodeFacadeContentAccessConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessOutboundPort;
import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;
import com.rboud.cps.core.DHTNode;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class NodeFacadeContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  public NodeFacadeContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, DHTNode.CONTENT_ACCESS_INBOUND_PORT_URI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeContentAccessOutboundPort port = new FacadeContentAccessOutboundPort(inboundPortURI, c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        NodeFacadeContentAccessConnector.class.getCanonicalName());
    return port;
  }

}