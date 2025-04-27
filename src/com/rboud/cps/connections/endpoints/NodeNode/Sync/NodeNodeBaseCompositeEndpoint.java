package com.rboud.cps.connections.endpoints.NodeNode.Sync;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

/**
 * Base composite endpoint managing synchronous connections between two DHT
 * nodes.
 * This endpoint combines content access and map-reduce capabilities for
 * synchronous
 * communication between peer nodes in the DHT network.
 *
 * @param <CAI> The type of content access interface
 * @param <MRI> The type of MapReduce interface
 */
public class NodeNodeBaseCompositeEndpoint<CAI extends ContentAccessSyncCI, MRI extends MapReduceSyncCI>
    extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<CAI, MRI> {

  /** Number of endpoints managed by this composite endpoint */
  private static final int N_ENDPOINTS = 2;

  /**
   * Creates a new composite endpoint for synchronous node-to-node communication.
   * Initializes both content access and map-reduce endpoints.
   */
  public NodeNodeBaseCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeSyncContentAccessEndPoint());
    this.addEndPoint(new NodeNodeSyncMapReduceEndPoint());
  }

  /**
   * Creates a new composite endpoint with a specified number of endpoints.
   * 
   * @param n The number of endpoints to create
   */
  public NodeNodeBaseCompositeEndpoint(int n) {
    super(n);
    this.addEndPoint(new NodeNodeSyncContentAccessEndPoint());
    this.addEndPoint(new NodeNodeSyncMapReduceEndPoint());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EndPointI<CAI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessSyncCI.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EndPointI<MRI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceSyncCI.class);
  }

}
