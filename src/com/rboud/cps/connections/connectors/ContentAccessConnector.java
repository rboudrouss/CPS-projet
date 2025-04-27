package com.rboud.cps.connections.connectors;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * Connector implementing asynchronous content access operations between
 * components.
 * Extends the synchronous connector to add asynchronous operations with result
 * callbacks.
 */
public class ContentAccessConnector extends ContentAccessSyncConnector implements ContentAccessCI {

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessI) this.offering).get(computationURI, key, caller);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    ((ContentAccessI) this.offering).put(computationURI, key, value, caller);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessI) this.offering).remove(computationURI, key, caller);
  }
}
