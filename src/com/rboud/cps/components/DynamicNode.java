package com.rboud.cps.components;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class, DHTManagementCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
    MapReduceResultReceptionCI.class })
public class DynamicNode extends AsyncNode implements DHTManagementI {

  protected DynamicNode(ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessI, MapReduceI> nextNodeCompositeEndpoint) throws Exception {
    super(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
  }

  @Override
  public void initialiseContent(NodeContentI content) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'initialiseContent'");
  }

  @Override
  public NodeStateI getCurrentState() throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getCurrentState'");
  }

  @Override
  public NodeContentI suppressNode() throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'suppressNode'");
  }

  @Override
  public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'split'");
  }

  @Override
  public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'merge'");
  }

  @Override
  public void computeChords(String computationURI, int numberOfChords) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'computeChords'");
  }

  @Override
  public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
      int offset) throws Exception {
        if (offset < 0) {
          throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (offset == 0) {
          ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> selfNode = new NodeNode

        }
  }

}
