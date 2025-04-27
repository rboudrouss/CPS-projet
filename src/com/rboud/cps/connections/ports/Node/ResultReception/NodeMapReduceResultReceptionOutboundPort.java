package com.rboud.cps.connections.ports.Node.ResultReception;

import java.io.Serializable;

import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

/**
 * Outbound port for sending MapReduce computation results back to callers.
 * Handles the transmission of intermediate and final results from MapReduce
 * operations.
 */
public class NodeMapReduceResultReceptionOutboundPort extends AbstractOutboundPort
    implements MapReduceResultReceptionCI {

  /**
   * Creates a new MapReduce result reception outbound port with the specified
   * URI.
   *
   * @param uri   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceResultReceptionOutboundPort(String uri, fr.sorbonne_u.components.ComponentI owner)
      throws Exception {
    super(uri, MapReduceResultReceptionCI.class, owner);
  }

  /**
   * Creates a new MapReduce result reception outbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeMapReduceResultReceptionOutboundPort(fr.sorbonne_u.components.ComponentI owner) throws Exception {
    super(MapReduceResultReceptionCI.class, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    ((MapReduceResultReceptionI) this.getConnector()).acceptResult(computationURI, emitterId, acc);
  }

}
