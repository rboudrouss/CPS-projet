package com.rboud.cps.connections.endpoints;

import com.rboud.cps.connections.ports.Client.ClientContentAccessOutboudPort;
import com.rboud.cps.connections.ports.Node.NodeContentAccessInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;

public class DHTContentAccessEndpoint extends BCMEndPoint<ContentAccessSyncCI> {

  public DHTContentAccessEndpoint() {
    super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    ClientContentAccessOutboudPort port = new ClientContentAccessOutboudPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }
  
}
