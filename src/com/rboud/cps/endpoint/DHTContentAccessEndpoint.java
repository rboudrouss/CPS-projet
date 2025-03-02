package com.rboud.cps.endpoint;

import com.rboud.cps.ports.ClientContentAccessOutboudPort;
import com.rboud.cps.ports.DHTContentAccessInboundPort;

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
    DHTContentAccessInboundPort port = new DHTContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String outboundPortURI, String inboundPortURI)
      throws Exception {
    ClientContentAccessOutboudPort port = new ClientContentAccessOutboudPort(outboundPortURI, c);

    return port;
  }
  
}
