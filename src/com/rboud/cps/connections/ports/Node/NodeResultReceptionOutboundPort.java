package com.rboud.cps.connections.ports.Node;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

public class NodeResultReceptionOutboundPort extends AbstractOutboundPort implements ResultReceptionCI {

  public NodeResultReceptionOutboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, ResultReceptionCI.class, owner);
  }

  public NodeResultReceptionOutboundPort(ComponentI owner) throws Exception {
    super(ResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, java.io.Serializable result) throws Exception {
    ((ResultReceptionI) this.getConnector()).acceptResult(computationURI, result);
  }

}
