package com.rboud.cps.components;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

public class Facade extends SyncFacade implements ResultReceptionI, MapReduceResultReceptionI {

  protected Facade(ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> nodeFacadeCompositeEndpoint,
      EndPointI<DHTServicesI> facadeClientDHTServicesEndpoint) throws Exception {
    super(nodeFacadeCompositeEndpoint, facadeClientDHTServicesEndpoint);
  }

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    throw new UnsupportedOperationException("Unimplemented method 'acceptResult'");
  }

  @Override
  public void acceptResult(String computationURI, Serializable result) throws Exception {
    throw new UnsupportedOperationException("Unimplemented method 'acceptResult'");
  }

}
