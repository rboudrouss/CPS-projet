package com.rboud.cps.connections.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class DHTCompositeEndpoint extends BCMCompositeEndPoint
    implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {
  
  private final static long serialVersionUID = 1L;

  private static final int N_ENDPOINTS = 2;

  public DHTCompositeEndpoint() {
    super(N_ENDPOINTS);
  }

  @Override
  public EndPointI<ContentAccessSyncCI> getContentAccessEndpoint() {
    return getEndPoint(ContentAccessSyncCI.class);
  }

  @Override
  public EndPointI<MapReduceSyncCI> getMapReduceEndpoint() {
    return getEndPoint(MapReduceSyncCI.class);
  }

}