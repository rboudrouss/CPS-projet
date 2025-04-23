package com.rboud.cps.components;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class Node extends SyncNode implements ContentAccessI, MapReduceI, ResultReceptionI {

  protected Node(ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nodeFacadeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> selfNodeCompositeEndpoint,
      ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nextNodeCompositeEndpoint)
      throws Exception {
    super(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);
    //TODO Auto-generated constructor stub
  }

  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'acceptResult'");
  }

  @Override
  public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'map'");
  }

  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'reduce'");
  }

  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'get'");
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'put'");
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'remove'");
  }

}
