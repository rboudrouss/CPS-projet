package com.rboud.cps.connections.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * Connector implementing synchronous content access operations between
 * components.
 */
public class ContentAccessSyncConnector extends AbstractConnector implements ContentAccessSyncCI {

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.offering).getSync(computationURI, key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return ((ContentAccessSyncI) this.offering).putSync(computationURI, key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.offering).removeSync(computationURI, key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearComputation(String computationURI) throws Exception {
    ((ContentAccessSyncI) this.offering).clearComputation(computationURI);
  }
}
