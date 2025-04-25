package com.rboud.cps.connections.endpoints.NodeFacade.NodeAsync;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class NodeFacadeCompositeEndpoint extends BCMCompositeEndPoint
    implements ContentNodeAsyncCompositeEndPointI<ContentAccessCI, MapReduceCI> {

  private static final int N_ENDPOINTS = 2;

  public NodeFacadeCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeFacadeContentAccessEndpoint());
    this.addEndPoint(new NodeFacadeMapReduceEndpoint());
  }

  @Override
  public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessCI.class);
  }

  @Override
  public EndPointI<MapReduceCI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceCI.class);
  }

}
