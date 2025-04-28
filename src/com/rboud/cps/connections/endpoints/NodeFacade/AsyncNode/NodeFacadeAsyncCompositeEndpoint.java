package com.rboud.cps.connections.endpoints.NodeFacade.AsyncNode;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * Composite endpoint that manages asynchronous connections between a node and
 * the facade.
 * This endpoint combines content access and map-reduce capabilities for
 * asynchronous
 * communication between DHT nodes and the facade component.
 * 
 * <p>
 * The endpoint creates and manages two sub-endpoints:
 * <ul>
 * <li>A content access endpoint for asynchronous content operations</li>
 * <li>A map-reduce endpoint for asynchronous distributed computations</li>
 * </ul>
 * </p>
 */
public class NodeFacadeAsyncCompositeEndpoint extends BCMCompositeEndPoint
    implements ContentNodeAsyncCompositeEndPointI<ContentAccessCI, MapReduceCI> {

  /** Number of endpoints managed by this composite endpoint */
  private static final int N_ENDPOINTS = 2;

  /**
   * Creates a new composite endpoint for asynchronous node-facade communication.
   * Initializes both content access and map-reduce endpoints that will handle
   * the communication between a node and the facade.
   */
  public NodeFacadeAsyncCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeFacadeContentAccessEndpoint());
    this.addEndPoint(new NodeFacadeMapReduceEndpoint());
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
