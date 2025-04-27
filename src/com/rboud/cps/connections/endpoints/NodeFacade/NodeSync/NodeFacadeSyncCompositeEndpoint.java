package com.rboud.cps.connections.endpoints.NodeFacade.NodeSync;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

/**
 * Composite endpoint that manages synchronous connections between a node and
 * the facade.
 * This endpoint combines content access and map-reduce capabilities for
 * synchronous
 * communication between DHT nodes and the facade component.
 * 
 * <p>
 * The endpoint creates and manages two sub-endpoints:
 * <ul>
 * <li>A content access endpoint for synchronous content operations</li>
 * <li>A map-reduce endpoint for synchronous distributed computations</li>
 * </ul>
 * </p>
 */
public class NodeFacadeSyncCompositeEndpoint extends BCMCompositeEndPoint
    implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {

  /** Number of endpoints managed by this composite endpoint */
  private final static int N_ENDPOINTS = 2;

  /**
   * Creates a new composite endpoint for synchronous node-facade communication.
   * 
   * Initializes both content access and map-reduce endpoints that will handle
   * the communication between a node and the facade.
   */
  public NodeFacadeSyncCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeFacadeSyncContentAccessEndpoint());
    this.addEndPoint(new NodeFacadeSyncMapReduceEndpoint());
  }

  /**
   * Gets the endpoint handling synchronous content access operations.
   * 
   * @return The content access endpoint interface
   */
  @Override
  public EndPointI<ContentAccessSyncCI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessSyncCI.class);
  }

  /**
   * Gets the endpoint handling synchronous map-reduce operations.
   * 
   * @return The map-reduce endpoint interface
   */
  @Override
  public EndPointI<MapReduceSyncCI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceSyncCI.class);
  }
}