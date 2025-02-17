package com.rboud.cps.core;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

public class DHTEndpoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> {
  private EndPointI<ContentAccessSyncI> contentAccessEndPoint;
  private EndPointI<MapReduceSyncI> mapReduceEndPoint;

  private boolean clientSideInitialise = false;

  public DHTEndpoint() {
    this.contentAccessEndPoint = null;
    this.mapReduceEndPoint = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialiseServerSide(Object serverSideEndPointOwner) {
    if (serverSideEndPointOwner instanceof ContentAccessSyncI)
      this.contentAccessEndPoint = (EndPointI<ContentAccessSyncI>) serverSideEndPointOwner;
    
    if (serverSideEndPointOwner instanceof MapReduceSyncI)
      this.mapReduceEndPoint = (EndPointI<MapReduceSyncI>) serverSideEndPointOwner;
    
    assert serverSideInitialised();
    assert clientSideInitialised();
  }

  /*
   * Note that this is the same as the server side
   */
  @Override
  public void initialiseClientSide(Object clientSideEndPointOwner) {
    assert clientSideEndPointOwner != null;
    assert serverSideInitialised();

    this.clientSideInitialise = true;
    assert clientSideInitialised();
  }

  @Override
  public boolean complete() {
    return this.contentAccessEndPoint != null && this.mapReduceEndPoint != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <I, J extends I> EndPointI<J> getEndPoint(Class<I> inter) {
    if (inter.equals(ContentAccessSyncI.class)) {
      return (EndPointI<J>) contentAccessEndPoint;
    } else if (inter.equals(MapReduceSyncI.class)) {
      return (EndPointI<J>) mapReduceEndPoint;
    }
    return null;
  }

  @Override
  public boolean serverSideInitialised() {
    return complete();
  }

  @Override
  public boolean clientSideInitialised() {
    return this.clientSideInitialise && serverSideInitialised();
  }

  @Override
  public void cleanUpServerSide() {
    this.contentAccessEndPoint = null;
    this.mapReduceEndPoint = null;
  }

  @Override
  public void cleanUpClientSide() {
    this.contentAccessEndPoint = null;
    this.mapReduceEndPoint = null;
  }

  @Override
  public EndPointI<ContentAccessSyncI> getContentAccessEndpoint() {
    return contentAccessEndPoint;
  }

  @Override
  public EndPointI<MapReduceSyncI> getMapReduceEndpoint() {
    return mapReduceEndPoint;
  }

  @Override
  public ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> copyWithSharable() {
    DHTEndpoint out = new DHTEndpoint();
    out.contentAccessEndPoint = this.contentAccessEndPoint;
    out.mapReduceEndPoint = this.mapReduceEndPoint;
    return out;
  }
}
