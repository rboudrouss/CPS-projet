package com.rboud.cps.connections.endpoints.FacadeClient;

import com.rboud.cps.connections.connectors.DHTServicesConnector;
import com.rboud.cps.connections.ports.Client.ClientDHTServicesOutboundPort;
import com.rboud.cps.connections.ports.Facade.FacadeDHTServicesInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class FacadeClientDHTServicesEndpoint extends BCMEndPoint<DHTServicesCI> {

  public FacadeClientDHTServicesEndpoint() {
    super(DHTServicesCI.class, DHTServicesCI.class);
  }

  public FacadeClientDHTServicesEndpoint(String inboundPortURI) {
    super(DHTServicesCI.class, DHTServicesCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeDHTServicesInboundPort port = new FacadeDHTServicesInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected DHTServicesCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    ClientDHTServicesOutboundPort port = new ClientDHTServicesOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
      port.getPortURI(),
      inboundPortURI,
      DHTServicesConnector.class.getCanonicalName()
    );
    return port;
  }
  
}
