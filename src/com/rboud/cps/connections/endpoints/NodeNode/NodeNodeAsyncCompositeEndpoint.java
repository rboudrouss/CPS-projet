package com.rboud.cps.connections.endpoints.NodeNode;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeAsyncCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class NodeNodeAsyncCompositeEndpoint<CAI extends ContentAccessCI, MRI extends MapReduceCI>
    extends NodeNodeBaseCompositeEndpoint<CAI, MRI> implements ContentNodeAsyncCompositeEndPointI<CAI, MRI> {

  public NodeNodeAsyncCompositeEndpoint() {
    super(4);
    this.addEndPoint(new NodeNodeContentAccessEndPoint());
    this.addEndPoint(new NodeNodeMapReduceResultReceptionEndpoint());
  }

}
