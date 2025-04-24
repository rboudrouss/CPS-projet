package com.rboud.cps.connections.ports.Node;

import java.io.Serializable;

import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class NodeMapReduceResultReceptionInboundPort extends AbstractInboundPort implements MapReduceResultReceptionCI {

  public NodeMapReduceResultReceptionInboundPort(String uri, fr.sorbonne_u.components.ComponentI owner)
      throws Exception {
    super(uri, MapReduceResultReceptionCI.class, owner);
  }

  public NodeMapReduceResultReceptionInboundPort(fr.sorbonne_u.components.ComponentI owner) throws Exception {
    super(MapReduceResultReceptionCI.class, owner);
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    this.getOwner().handleRequest(c -> {
      ((MapReduceResultReceptionCI) c).acceptResult(computationURI, emitterId, acc);
      return null;
    });
  }

}
