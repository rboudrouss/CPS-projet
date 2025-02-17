package com.rboud.cps.connections;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class DHTContentAccessInboundPort extends AbstractInboundPort implements ContentAccessSyncCI {

  public DHTContentAccessInboundPort(ComponentI owner) throws Exception {
    super(ContentAccessSyncCI.class, owner);
  }

  public DHTContentAccessInboundPort(String URI, ComponentI owner) throws Exception {
    super(URI, ContentAccessSyncCI.class, owner);
  }

  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncCI) c).getSync(computationURI, key));
  }

  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncCI) c).putSync(computationURI, key, value));
  }

  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return this.getOwner().handleRequest((c) -> ((ContentAccessSyncCI) c).removeSync(computationURI, key));
  }

  @Override
  public void clearComputation(String computationURI) throws Exception {
    this.getOwner().handleRequest((c) -> {
      ((ContentAccessSyncCI) c).clearComputation(computationURI);
      return null;
    });
  }
}
