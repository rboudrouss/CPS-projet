package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

/**
 * Inbound port handling MapReduce result reception for the facade.
 * Receives and processes computation results from MapReduce operations.
 */
public class FacadeMapReduceResultReceptionInboundPort extends AbstractInboundPort
    implements MapReduceResultReceptionCI {

  /**
   * Creates a new MapReduce result reception inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeMapReduceResultReceptionInboundPort(ComponentI owner)
      throws Exception {
    super(MapReduceResultReceptionCI.class, owner);
  }

  /**
   * Creates a new MapReduce result reception inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeMapReduceResultReceptionInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(URI, MapReduceResultReceptionCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceResultReceptionI) c).acceptResult(computationURI, emitterId, acc);
      return null;
    });
  }

}
