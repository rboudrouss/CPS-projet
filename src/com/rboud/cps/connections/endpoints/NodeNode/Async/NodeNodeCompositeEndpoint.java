package com.rboud.cps.connections.endpoints.NodeNode.Async;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class NodeNodeCompositeEndpoint<CAI extends ContentAccessCI, MRI extends MapReduceCI>
    extends BCMCompositeEndPoint
    implements ContentNodeAsyncCompositeEndPointI<CAI, MRI> {

  private static final int N_ENDPOINTS = 2;

  public NodeNodeCompositeEndpoint() {
    super(N_ENDPOINTS);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceEndPoint());
  }

  @Override
  public EndPointI<CAI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessCI.class);
  }

  @Override
  public EndPointI<MRI> getMapReduceEndpoint() {
    return this.getEndPoint(MapReduceCI.class);
  }

}
