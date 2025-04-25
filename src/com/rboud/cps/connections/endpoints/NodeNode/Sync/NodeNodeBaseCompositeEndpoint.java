package com.rboud.cps.connections.endpoints.NodeNode.Sync;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeNodeBaseCompositeEndpoint<CAI extends ContentAccessSyncCI, MRI extends MapReduceSyncCI>
    extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<CAI, MRI> {
    
  private static final int N_ENDPOINTS = 2;

  public NodeNodeBaseCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeSyncContentAccessEndPoint());
    this.addEndPoint(new NodeNodeSyncMapReduceEndPoint());
  }

  public NodeNodeBaseCompositeEndpoint(int n) {
    super(n);
    this.addEndPoint(new NodeNodeSyncContentAccessEndPoint());
    this.addEndPoint(new NodeNodeSyncMapReduceEndPoint());
  }

  @Override
  public EndPointI<CAI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessSyncCI.class);
  }

  @Override
  public EndPointI<MRI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceSyncCI.class);
  }

}
