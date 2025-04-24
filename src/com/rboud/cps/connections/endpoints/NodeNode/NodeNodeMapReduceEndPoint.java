package com.rboud.cps.connections.endpoints.NodeNode;

import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Node.NodeMapReduceSyncInboundPort;
import com.rboud.cps.connections.ports.Node.NodeMapReduceOutboundPort;

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
    NodeMapReduceSyncInboundPort port = new NodeMapReduceSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceOutboundPort port = new NodeMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceConnector.class.getCanonicalName());
    return port;
  }

}
