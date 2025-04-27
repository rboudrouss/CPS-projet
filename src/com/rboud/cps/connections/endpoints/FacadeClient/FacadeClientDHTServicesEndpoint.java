package com.rboud.cps.connections.endpoints.FacadeClient;

import com.rboud.cps.connections.connectors.DHTServicesConnector;
import com.rboud.cps.connections.ports.Client.ClientDHTServicesOutboundPort;
import com.rboud.cps.connections.ports.Facade.FacadeDHTServicesInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

/**
 * Endpoint managing connections between clients and the DHT facade.
 * Creates and manages inbound and outbound ports for DHT service
 * communications.
 */
public class FacadeClientDHTServicesEndpoint extends BCMEndPoint<DHTServicesCI> {

  /**
   * Creates a new endpoint with default configuration.
   */
  public FacadeClientDHTServicesEndpoint() {
    super(DHTServicesCI.class, DHTServicesCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public FacadeClientDHTServicesEndpoint(String inboundPortURI) {
    super(DHTServicesCI.class, DHTServicesCI.class, inboundPortURI);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeDHTServicesInboundPort port = new FacadeDHTServicesInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DHTServicesCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    ClientDHTServicesOutboundPort port = new ClientDHTServicesOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        DHTServicesConnector.class.getCanonicalName());
    return port;
  }

}
