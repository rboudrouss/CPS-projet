package com.rboud.cps.connections.endpoints.NodeNode.Dynamic;

import com.rboud.cps.connections.endpoints.NodeNode.Async.NodeNodeAsyncContentAccessEndPoint;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;

public class NodeNodeDynamicCompositeEndPoint extends BCMCompositeEndPoint
    implements ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> {
  private static final int NB_ENDPOINTS = 3;

  public NodeNodeDynamicCompositeEndPoint() {
    super(NB_ENDPOINTS);
    this.addEndPoint(new NodeNodeManagementEndPoint());
    this.addEndPoint(new NodeNodeParallelMapReduceEndpoint());
    this.addEndPoint(new NodeNodeAsyncContentAccessEndPoint());
  }

  @Override
  public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
    return this.getEndPoint(ContentAccessCI.class);
  }

  @Override
  public EndPointI<ParallelMapReduceCI> getMapReduceEndpoint() {
    return this.getEndPoint(ParallelMapReduceCI.class);
  }

  @Override
  public EndPointI<DHTManagementCI> getDHTManagementEndpoint() {
    return this.getEndPoint(DHTManagementCI.class);
  }

}