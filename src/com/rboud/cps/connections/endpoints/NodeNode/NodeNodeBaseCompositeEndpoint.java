package com.rboud.cps.connections.endpoints.NodeNode;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeNodeBaseCompositeEndpoint<CAI extends ContentAccessSyncCI, MRI extends MapReduceSyncCI>
    extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<CAI, MRI> {

  public NodeNodeBaseCompositeEndpoint() {
    super(2);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceEndPoint());
  }

  public NodeNodeBaseCompositeEndpoint(int n) {
    super(n);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceEndPoint());
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
