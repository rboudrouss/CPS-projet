package com.rboud.cps.connections.ports.Node;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeContentAccessOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {

  public NodeContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
    super(uri, MapReduceSyncCI.class, owner);
  }

  public NodeContentAccessOutboundPort(ComponentI owner) throws Exception {
    super(MapReduceSyncCI.class, owner);
  }

  @Override
  public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessCI) this.connector).getSync(computationURI, key);
  }

  @Override
  public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
    return ((ContentAccessCI) this.connector).putSync(computationURI, key, value);
  }

  @Override
  public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
    return ((ContentAccessCI) this.connector).removeSync(computationURI, key);
  }

  @Override
  public void clearComputation(String computationURI) throws Exception {
    ((ContentAccessCI) this.connector).clearComputation(computationURI);
  }

}
