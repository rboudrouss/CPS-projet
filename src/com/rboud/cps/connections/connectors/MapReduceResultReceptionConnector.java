package com.rboud.cps.connections.connectors;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

public class MapReduceResultReceptionConnector  extends AbstractConnector implements MapReduceResultReceptionI {

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    ((MapReduceResultReceptionI) this.offering).acceptResult(computationURI, emitterId, acc);
  }
  
}
