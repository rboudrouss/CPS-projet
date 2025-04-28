package com.rboud.cps.connections.endpoints.NodeNode.Async;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * Composite endpoint managing asynchronous connections between two DHT nodes.
 * This endpoint combines content access and map-reduce capabilities for
 * asynchronous
 * communication between peer nodes in the DHT network.
 * 
 * <p>
 * The endpoint creates and manages two sub-endpoints:
 * <ul>
 * <li>A content access endpoint for asynchronous content operations</li>
 * <li>A map-reduce endpoint for asynchronous distributed computations</li>
 * </ul>
 * </p>
 */
public class NodeNodeAsyncCompositeEndpoint
    extends BCMCompositeEndPoint
    implements ContentNodeAsyncCompositeEndPointI<ContentAccessCI, MapReduceCI> {

  /** Number of endpoints managed by this composite endpoint */
  private static final int N_ENDPOINTS = 2;

  /**
   * Creates a new composite endpoint for asynchronous node-to-node communication.
   * Initializes both content access and map-reduce endpoints that will handle
   * the communication between two DHT nodes.
   */
  public NodeNodeAsyncCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeAsyncContentAccessEndPoint());
    this.addEndPoint(new NodeNodeAsyncMapReduceEndPoint());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EndPointI<MapReduceCI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceCI.class);
  }

}
