
package com.rboud.cps.connections.ports.Node;

import java.io.Serializable;

import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

public class NodeMapReduceResultReceptionOutboundPort extends AbstractOutboundPort
    implements MapReduceResultReceptionCI {

  public NodeMapReduceResultReceptionOutboundPort(String uri, fr.sorbonne_u.components.ComponentI owner)
      throws Exception {
    super(uri, MapReduceResultReceptionCI.class, owner);
  }

  public NodeMapReduceResultReceptionOutboundPort(fr.sorbonne_u.components.ComponentI owner) throws Exception {
    super(MapReduceResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    ((MapReduceResultReceptionI) this.getConnector()).acceptResult(computationURI, emitterId, acc);
  }

}
