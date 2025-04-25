package com.rboud.cps.connections.ports.Node.Async;

import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncOutboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class NodeContentAccessOutboundPort extends NodeContentAccessSyncOutboundPort implements ContentAccessCI {

  public NodeContentAccessOutboundPort(ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, owner);
  }

  public NodeContentAccessOutboundPort(String URI, ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, URI, owner);
  }

  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessCI) this.getConnector()).get(computationURI, key, caller);
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    ((ContentAccessCI) this.getConnector()).put(computationURI, key, value, caller);
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    ((ContentAccessCI) this.getConnector()).remove(computationURI, key, caller);
  }

}
