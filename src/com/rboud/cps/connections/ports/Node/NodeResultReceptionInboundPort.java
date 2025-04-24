package com.rboud.cps.connections.ports.Node;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

public class NodeResultReceptionInboundPort extends AbstractInboundPort implements ResultReceptionCI {

  public NodeResultReceptionInboundPort(ComponentI owner)
      throws Exception {
    super(ResultReceptionCI.class, owner);
  }

  public NodeResultReceptionInboundPort(String uri, ComponentI owner)
      throws Exception {
    super(uri, ResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((ResultReceptionI) c).acceptResult(computationURI, result);
      return null;
    });
  }

}
