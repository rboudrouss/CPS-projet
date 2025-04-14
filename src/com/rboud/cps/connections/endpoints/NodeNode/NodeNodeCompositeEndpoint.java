package com.rboud.cps.connections.endpoints.NodeNode;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

public class NodeNodeCompositeEndpoint extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> {
  private final static int N_ENDPOINTS = 2;

  public NodeNodeCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceEndPoint());
  }

  @Override
  public EndPointI<ContentAccessSyncI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessSyncI.class);
  }

  @Override
  public EndPointI<MapReduceSyncI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceSyncI.class);
  }
  
}
