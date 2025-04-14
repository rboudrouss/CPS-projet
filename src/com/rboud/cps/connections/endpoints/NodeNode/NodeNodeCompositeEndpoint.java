package com.rboud.cps.connections.endpoints.NodeNode;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeNodeCompositeEndpoint extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {
  private final static int N_ENDPOINTS = 2;

  public NodeNodeCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceEndPoint());
  }

  @Override
  public EndPointI<ContentAccessSyncCI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessSyncCI.class);
  }

  @Override
  public EndPointI<MapReduceSyncCI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceSyncCI.class);
  }
  
}
