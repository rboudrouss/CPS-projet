package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

/**
 * Inbound port handling content access result reception for the facade.
 * Receives and processes results from content management operations.
 */
public class FacadeResultReceptionInboundPort extends AbstractInboundPort implements ResultReceptionCI {

  /**
   * Creates a new result reception inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeResultReceptionInboundPort(ComponentI owner)
      throws Exception {
    super(ResultReceptionCI.class, owner);
  }

  /**
   * Creates a new result reception inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeResultReceptionInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(URI, ResultReceptionCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    this.getOwner().handleRequest(
        (c) -> {
          ((ResultReceptionI) c).acceptResult(computationURI, result);
          return null;
        });
  }

}
