package com.rboud.cps.core;

import fr.sorbonne_u.components.endpoints.EndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.endpoints.POJOEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

public class DHTEndpoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> {
  private EndPoint<ContentAccessSyncI> contentAccessEndPoint;
  private EndPoint<MapReduceSyncI> mapReduceEndPoint;

  public DHTEndpoint() {
    this.contentAccessEndPoint = new POJOEndPoint<>(ContentAccessSyncI.class);
    this.mapReduceEndPoint = new POJOEndPoint<>(MapReduceSyncI.class);
  }

  @Override
  public void initialiseServerSide(Object serverSideEndPointOwner) {
    assert serverSideEndPointOwner != null;

    this.contentAccessEndPoint.initialiseServerSide(serverSideEndPointOwner);
    this.mapReduceEndPoint.initialiseServerSide(serverSideEndPointOwner);

    assert serverSideInitialised();
  }

  @Override
  public void initialiseClientSide(Object clientSideEndPointOwner) {
    assert clientSideEndPointOwner != null;
    assert serverSideInitialised();

    this.contentAccessEndPoint.initialiseClientSide(clientSideEndPointOwner);
    this.mapReduceEndPoint.initialiseClientSide(clientSideEndPointOwner);

    assert clientSideInitialised();
  }

  @Override
  public boolean complete() {
    return this.contentAccessEndPoint != null && this.mapReduceEndPoint != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <I, J extends I> EndPointI<J> getEndPoint(Class<I> inter) {
    if (ContentAccessSyncI.class.isAssignableFrom(inter)) {
      return (EndPointI<J>) contentAccessEndPoint;
    } else if (MapReduceSyncI.class.isAssignableFrom(inter)) {
      return (EndPointI<J>) mapReduceEndPoint;
    }
    return null;
  }

  @Override
  public boolean serverSideInitialised() {
    return complete() && this.contentAccessEndPoint.serverSideInitialised() && this.mapReduceEndPoint.serverSideInitialised();
  }

  @Override
  public boolean clientSideInitialised() {
    return complete() && this.contentAccessEndPoint.clientSideInitialised() && this.mapReduceEndPoint.clientSideInitialised() && this.serverSideInitialised();
  }

  @Override
  public void cleanUpServerSide() {
    this.contentAccessEndPoint.cleanUpServerSide();
    this.mapReduceEndPoint.cleanUpServerSide();
  }

  @Override
  public void cleanUpClientSide() {
    this.contentAccessEndPoint.cleanUpClientSide();
    this.mapReduceEndPoint.cleanUpClientSide();
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
