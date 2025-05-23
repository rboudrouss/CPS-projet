package com.rboud.cps.connections.ports.Node.Sync;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * Inbound port handling synchronous content access operations for DHT nodes.
 * Processes incoming requests for synchronous content management operations.
 */
public class NodeContentAccessSyncInboundPort extends AbstractInboundPort implements ContentAccessSyncCI {

  /**
   * Creates a new synchronous content access inbound port.
   *
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(ComponentI owner) throws Exception {
    super(ContentAccessSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous content access inbound port with the specified URI.
   *
   * @param URI   The unique URI for this port
   * @param owner The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, ContentAccessSyncCI.class, owner);
  }

  /**
   * Creates a new synchronous content access inbound port with specified
   * interface and URI.
   *
   * @param implementedInterface The interface implemented by this port
   * @param URI                  The unique URI for this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(Class<? extends ContentAccessSyncCI> implementedInterface, String URI,
      ComponentI owner) throws Exception {
    super(URI, implementedInterface, owner);
  }

  /**
   * Creates a new synchronous content access inbound port with specified
   * interface.
   *
   * @param implementedInterface The interface implemented by this port
   * @param owner                The component owner of this port
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(Class<? extends ContentAccessSyncCI> implementedInterface, ComponentI owner)
      throws Exception {
    super(implementedInterface, owner);
  }

  /**
   * Creates a new synchronous content access inbound port with specified
   * interface, URI, and executor service URI.
   *
   * @param uri                  The unique URI for this port
   * @param implementedInterface The interface implemented by this port
   * @param owner                The component owner of this port
   * @param pluginURI            The plugin URI for the executor service
   * @param executorServiceUri   The executor service URI
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(String uri, Class<? extends ContentAccessSyncCI> implementedInterface,
      ComponentI owner, String pluginURI, String executorServiceUri) throws Exception {
    super(uri, implementedInterface, owner, pluginURI, executorServiceUri);
  }

  /**
   * Creates a new synchronous content access inbound port with specified
   * interface and executor service URI.
   *
   * @param implementedInterface The interface implemented by this port
   * @param owner                The component owner of this port
   * @param pluginURI            The plugin URI for the executor service
   * @param executorServiceUri   The executor service URI
   * @throws Exception If port creation fails
   */
  public NodeContentAccessSyncInboundPort(Class<? extends ContentAccessSyncCI> implementedInterface,
      ComponentI owner, String pluginURI, String executorServiceUri) throws Exception {
    super(implementedInterface, owner, pluginURI, executorServiceUri);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncI) c).getSync(computationURI, key));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncI) c).putSync(computationURI, key, value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncI) c).removeSync(computationURI, key));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearComputation(String computationURI) throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((ContentAccessSyncI) c).clearComputation(computationURI);
      return null;
    });
  }
}
