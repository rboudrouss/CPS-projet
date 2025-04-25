package com.rboud.cps.connections.ports.Facade;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

public class FacadeMapReduceResultReceptionInboundPort extends AbstractInboundPort implements MapReduceResultReceptionI {

  public FacadeMapReduceResultReceptionInboundPort(ComponentI owner)
      throws Exception {
    super(MapReduceResultReceptionCI.class, owner);
  }

  public FacadeMapReduceResultReceptionInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(URI, MapReduceResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceResultReceptionI) c).acceptResult(computationURI, emitterId, acc);
      return null;
    });
  }
  
}
