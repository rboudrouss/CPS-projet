package com.rboud.cps.connections.endpoints.NodeFacade;

import com.rboud.cps.connections.connectors.NodeFacade.NodeFacadeMapReduceConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceOutboundPort;
import com.rboud.cps.connections.ports.Node.NodeMapReduceInboundPort;
import com.rboud.cps.core.DHTNode;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeFacadeMapReduceEndpoint extends BCMEndPoint<MapReduceSyncCI> {

  public NodeFacadeMapReduceEndpoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class, DHTNode.MAP_REDUCE_INBOUND_PORT_URI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceOutboundPort port = new FacadeMapReduceOutboundPort(inboundPortURI, c);
    port.publishPort();
    c.doPortConnection(
      port.getPortURI(),
      inboundPortURI,
      NodeFacadeMapReduceConnector.class.getCanonicalName()
    );
    return port;
  }

}
