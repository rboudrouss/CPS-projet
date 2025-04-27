package com.rboud.cps.connections.ports.Facade;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * Outbound port implementing synchronous content access operations for the
 * facade.
 * Provides methods for synchronous content management operations (get, put,
 * remove).
 */
public class FacadeContentAccessSyncOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {

  /**
   * Creates a new synchronous content access outbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeContentAccessSyncOutboundPort(ComponentI owner) throws Exception {
    super(ContentAccessSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous content access outbound port with the specified
   * URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeContentAccessSyncOutboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, ContentAccessSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous content access outbound port with specified
   * interface and URI.
   *
   * @param implementedInterface The interface implemented by this port
   * @param URI                  The unique URI for this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeContentAccessSyncOutboundPort(Class<? extends ContentAccessSyncCI> implementedInterface, String URI,
      ComponentI owner) throws Exception {
    super(URI, implementedInterface, owner);
  }

  /**
   * Creates a new synchronous content access outbound port with specified
   * interface.
   *
   * @param implementedInterface The interface implemented by this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public FacadeContentAccessSyncOutboundPort(Class<? extends ContentAccessSyncCI> implementedInterface,
      ComponentI owner)
      throws Exception {
    super(implementedInterface, owner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).getSync(computationURI, key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).putSync(computationURI, key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).removeSync(computationURI, key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearComputation(String computationURI) throws Exception {
    ((ContentAccessSyncI) this.getConnector()).clearComputation(computationURI);
  }

}
