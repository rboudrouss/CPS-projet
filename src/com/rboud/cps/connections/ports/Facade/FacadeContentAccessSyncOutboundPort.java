package com.rboud.cps.connections.ports.Facade;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class FacadeContentAccessSyncOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {

  public FacadeContentAccessSyncOutboundPort(ComponentI owner) throws Exception {
    super(ContentAccessSyncCI.class, owner);
  }

  public FacadeContentAccessSyncOutboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, ContentAccessSyncCI.class, owner);
  }

  public FacadeContentAccessSyncOutboundPort(Class<? extends ContentAccessSyncCI> implementedInterface, String URI,
      ComponentI owner) throws Exception {
    super(URI, implementedInterface, owner);
  }

  public FacadeContentAccessSyncOutboundPort(Class<? extends ContentAccessSyncCI> implementedInterface, ComponentI owner)
      throws Exception {
    super(implementedInterface, owner);
  }

  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).getSync(computationURI, key);
  }

  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).putSync(computationURI, key, value);
  }

  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.getConnector()).removeSync(computationURI, key);
  }

  @Override
  public void clearComputation(String computationURI) throws Exception {
    ((ContentAccessSyncI) this.getConnector()).clearComputation(computationURI);
  }
  
}
