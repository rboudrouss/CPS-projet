package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

public class FacadeResultReceptionInboundPort extends AbstractInboundPort implements ResultReceptionCI {

  public FacadeResultReceptionInboundPort(ComponentI owner)
      throws Exception {
    super(ResultReceptionCI.class, owner);
  }

  public FacadeResultReceptionInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(URI, ResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    this.getOwner().handleRequest(
        (c) -> {
          ((ResultReceptionI) c).acceptResult(computationURI, result);
          return null;
        });
  }

}
