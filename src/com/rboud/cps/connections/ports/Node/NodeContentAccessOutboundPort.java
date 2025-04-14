package com.rboud.cps.connections.ports.Node;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class NodeContentAccessOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {

  public NodeContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, ContentAccessSyncCI.class, owner);
  }

  public NodeContentAccessOutboundPort(ComponentI owner) throws Exception {
    super(ContentAccessSyncCI.class, owner);
  }

  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.connector).getSync(computationURI, key);
  }

  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return ((ContentAccessSyncI) this.connector).putSync(computationURI, key, value);
  }

  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessSyncI) this.connector).removeSync(computationURI, key);
  }

  @Override
  public void clearComputation(String computationURI) throws Exception {
    ((ContentAccessSyncI) this.connector).clearComputation(computationURI);
  }

}
