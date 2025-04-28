package com.rboud.cps.connections.ports.Node.Dynamic;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

public class NodeManagementInboundPort extends AbstractInboundPort implements DHTManagementCI {

  public NodeManagementInboundPort(ComponentI owner) throws Exception {
    super(DHTManagementCI.class, owner);
  }

  public NodeManagementInboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, DHTManagementCI.class, owner);
  }

  @Override
  public void initialiseContent(NodeContentI content) throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((DHTManagementI) c).initialiseContent(content);
      return null;
    });
  }

  @Override
  public NodeStateI getCurrentState() throws Exception {
    return this.getOwner().handleRequest((c) -> {
      return ((DHTManagementI) c).getCurrentState();
    });
  }

  @Override
  public NodeContentI suppressNode() throws Exception {
    return this.getOwner().handleRequest((c) -> {
      return ((DHTManagementI) c).suppressNode();
    });
  }

  @Override
  public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((DHTManagementI) c).split(computationURI, loadPolicy, caller);
      return null;
    });
  }

  @Override
  public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((DHTManagementI) c).merge(computationURI, loadPolicy, caller);
      return null;
    });
  }

  @Override
  public void computeChords(String computationURI, int numberOfChords) throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((DHTManagementI) c).computeChords(computationURI, numberOfChords);
      return null;
    });
  }

  @Override
  public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
      int offset) throws Exception {
    return this.getOwner().handleRequest((c) -> {
      return ((DHTManagementI) c).getChordInfo(offset);
    });
  }

}
