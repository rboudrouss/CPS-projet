package com.rboud.cps.connections.connectors;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

/**
 * Connector for receiving computation results between components.
 * Handles result reception from distributed operations.
 */
public class ResultReceptionConnector extends AbstractConnector implements ResultReceptionI {

  /**
   * {@inheritDoc}
   */
  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    ((ResultReceptionCI) this.offering).acceptResult(computationURI, result);
  }

}
