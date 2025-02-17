package com.rboud.cps.core;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;

public class DHTEndpoint implements ContentNodeBaseCompositeEndPointI<DHTNode, DHTNode> {

  

  @Override
  public void initialiseServerSide(Object serverSideEndPointOwner) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'initialiseServerSide'");
  }

  @Override
  public void initialiseClientSide(Object clientSideEndPointOwner) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'initialiseClientSide'");
  }

  @Override
  public boolean complete() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'complete'");
  }

  @Override
  public <I, J extends I> EndPointI<J> getEndPoint(Class<I> inter) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getEndPoint'");
  }

  @Override
  public boolean serverSideInitialised() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'serverSideInitialised'");
  }

  @Override
  public boolean clientSideInitialised() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'clientSideInitialised'");
  }

  @Override
  public void cleanUpServerSide() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'cleanUpServerSide'");
  }

  @Override
  public void cleanUpClientSide() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'cleanUpClientSide'");
  }

  @Override
  public EndPointI<DHTNode> getContentAccessEndpoint() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getContentAccessEndpoint'");
  }

  @Override
  public EndPointI<DHTNode> getMapReduceEndpoint() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getMapReduceEndpoint'");
  }

  @Override
  public ContentNodeBaseCompositeEndPointI<DHTNode, DHTNode> copyWithSharable() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'copyWithSharable'");
  }
}
