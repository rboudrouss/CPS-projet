package com.rboud.cps.connections.endpoints.NodeFacade.NodeAsync;

import com.rboud.cps.connections.connectors.MapReduceConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceOutboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeMapReduceInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class NodeFacadeMapReduceEndpoint extends BCMEndPoint<MapReduceCI> {

  public NodeFacadeMapReduceEndpoint() {
    super(MapReduceCI.class, MapReduceCI.class);
  }

  public NodeFacadeMapReduceEndpoint(String inboundPortURI) {
    super(MapReduceCI.class, MapReduceCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceInboundPort port = new NodeMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceOutboundPort port = new FacadeMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceConnector.class.getCanonicalName());
    return port;
  }
}
