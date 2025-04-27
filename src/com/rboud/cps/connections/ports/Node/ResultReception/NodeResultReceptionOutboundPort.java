package com.rboud.cps.connections.ports.Node.ResultReception;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

/**
 * Outbound port for sending content access operation results back to callers.
 * Handles the transmission of results from content management operations.
 */
public class NodeResultReceptionOutboundPort extends AbstractOutboundPort implements ResultReceptionCI {

  /**
   * Creates a new result reception outbound port with the specified URI.
   *
   * @param uri   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeResultReceptionOutboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, ResultReceptionCI.class, owner);
  }

  /**
   * Creates a new result reception outbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeResultReceptionOutboundPort(ComponentI owner) throws Exception {
    super(ResultReceptionCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void acceptResult(String computationURI, java.io.Serializable result) throws Exception {
    ((ResultReceptionI) this.getConnector()).acceptResult(computationURI, result);
  }

}
