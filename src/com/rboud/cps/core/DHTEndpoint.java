package com.rboud.cps.core;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

public class DHTEndpoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> {
  private boolean initialized;
  private EndPointI<ContentAccessSyncI> contentAccessEndPoint;
  private EndPointI<MapReduceSyncI> mapReduceEndPoint;

  public DHTEndpoint() {
    this.initialized = false;
    this.contentAccessEndPoint = null;
    this.mapReduceEndPoint = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialiseServerSide(Object serverSideEndPointOwner) {
    if (serverSideEndPointOwner instanceof ContentAccessSyncI)
      this.contentAccessEndPoint = (EndPointI<ContentAccessSyncI>) serverSideEndPointOwner;
    else
      System.out.println(
          "WARNING DHTEndpoint#initialiseServerSide : serverSideEndPointOwner is not an instance of ContentAccessSyncI");
    if (serverSideEndPointOwner instanceof MapReduceSyncI)
      this.mapReduceEndPoint = (EndPointI<MapReduceSyncI>) serverSideEndPointOwner;
    else
      System.out.println(
          "WARNING DHTEndpoint#initialiseServerSide : serverSideEndPointOwner is not an instance of MapReduceSyncI");

    initialized = true;
  }

  /*
   * Note that this is the same as the server side
   */
  @SuppressWarnings("unchecked")
  @Override
  public void initialiseClientSide(Object clientSideEndPointOwner) {
    if (clientSideEndPointOwner instanceof ContentAccessSyncI)
      this.contentAccessEndPoint = (EndPointI<ContentAccessSyncI>) clientSideEndPointOwner;
    else
      System.out.println(
          "WARNING DHTEndpoint#initialiseClientSide : clientSideEndPointOwner is not an instance of ContentAccessSyncI");
    if (clientSideEndPointOwner instanceof MapReduceSyncI)
      this.mapReduceEndPoint = (EndPointI<MapReduceSyncI>) clientSideEndPointOwner;
    else
      System.out.println(
          "WARNING DHTEndpoint#initialiseClientSide : clientSideEndPointOwner is not an instance of MapReduceSyncI");

    initialized = true;
  }

  @Override
  public boolean complete() {
    return initialized;
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
    return initialized;
  }

  @Override
  public boolean clientSideInitialised() {
    return initialized;
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
    out.initialized = this.initialized;
    out.contentAccessEndPoint = this.contentAccessEndPoint;
    out.mapReduceEndPoint = this.mapReduceEndPoint;
    return out;
  }
}
