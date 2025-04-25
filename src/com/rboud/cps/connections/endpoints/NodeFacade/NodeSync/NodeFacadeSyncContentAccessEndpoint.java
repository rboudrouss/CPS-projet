package com.rboud.cps.connections.endpoints.NodeFacade.NodeSync;

import com.rboud.cps.connections.connectors.ContentAccessSyncConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessSyncOutboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class NodeFacadeSyncContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  public NodeFacadeSyncContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  public NodeFacadeSyncContentAccessEndpoint(String inboundPortURI) {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessSyncInboundPort port = new NodeContentAccessSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeContentAccessSyncOutboundPort port = new FacadeContentAccessSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessSyncConnector.class.getCanonicalName());
    return port;
  }

}