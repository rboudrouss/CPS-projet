package com.rboud.cps.connections.endpoints.NodeFacade;

import fr.sorbonne_u.components.endpoints.CompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeFacadeCompositeEndpoint extends CompositeEndPoint
    implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {
  private final static int N_ENDPOINTS = 2;

  public NodeFacadeCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeFacadeContentAccessEndpoint());
    this.addEndPoint(new NodeFacadeMapReduceEndpoint());
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