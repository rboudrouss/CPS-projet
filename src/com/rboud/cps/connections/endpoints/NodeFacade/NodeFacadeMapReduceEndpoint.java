package com.rboud.cps.connections.endpoints.NodeFacade;

import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceOutboundPort;
import com.rboud.cps.connections.ports.Node.NodeMapReduceSyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeFacadeMapReduceEndpoint extends BCMEndPoint<MapReduceSyncCI> {

  public NodeFacadeMapReduceEndpoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  public NodeFacadeMapReduceEndpoint(String inboundPortURI) {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceSyncInboundPort port = new NodeMapReduceSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceOutboundPort port = new FacadeMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
      port.getPortURI(),
      inboundPortURI,
      MapReduceConnector.class.getCanonicalName()
    );
    return port;
  }

}
