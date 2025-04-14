package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.ports.Node.NodeMapReduceInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeNodeMapReduceEndPoint extends BCMEndPoint<MapReduceSyncCI> {

  public NodeNodeMapReduceEndPoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceSyncCI.class.getCanonicalName());
    return port;
  }

}
