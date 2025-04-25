package com.rboud.cps.connections.ports.Node.Async;

import com.rboud.cps.connections.ports.Node.Sync.NodeContentAccessSyncInboundPort;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class NodeContentAccessInboundPort extends NodeContentAccessSyncInboundPort implements ContentAccessCI {

  public NodeContentAccessInboundPort(ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, owner);
  }

  public NodeContentAccessInboundPort(String URI, ComponentI owner)
      throws Exception {
    super(ContentAccessCI.class, URI, owner);
  }

  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.getOwner().runTask(
        (c) -> {
          try {
            ((ContentAccessI) c).get(computationURI, key, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    this.getOwner().runTask(
        (c) -> {
          try {
            ((ContentAccessI) c).put(computationURI, key, value, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    this.getOwner().runTask(
        (c) -> {
          try {
            ((ContentAccessI) c).remove(computationURI, key, caller);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

}
