package com.rboud.cps.endpoint;

import java.util.ArrayList;

import com.rboud.cps.hack.ContentNodeBaseCompositeEndPointI;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class DHTCompositeEndpoint extends BCMCompositeEndPoint
    implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {

  public DHTCompositeEndpoint() {
    super(2);
  }

  public DHTCompositeEndpoint(ArrayList<EndPointI<?>> initialEndPoints) {
    super(initialEndPoints);
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