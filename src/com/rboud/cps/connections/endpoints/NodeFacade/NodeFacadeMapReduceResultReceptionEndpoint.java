package com.rboud.cps.connections.endpoints.NodeFacade;

import com.rboud.cps.connections.connectors.MapReduceResultReceptionConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceResultReceptionInboundPort;
import com.rboud.cps.connections.ports.Node.NodeMapReduceResultReceptionOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class NodeFacadeMapReduceResultReceptionEndpoint extends BCMEndPoint<MapReduceResultReceptionCI> {

  public NodeFacadeMapReduceResultReceptionEndpoint() {
    super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceResultReceptionInboundPort p = new FacadeMapReduceResultReceptionInboundPort(inboundPortURI, c);
    p.publishPort();
    return p;
  }

  @Override
  protected MapReduceResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceResultReceptionOutboundPort p = new NodeMapReduceResultReceptionOutboundPort(inboundPortURI, c);
    p.publishPort();
    c.doPortConnection(
        p.getPortURI(),
        inboundPortURI,
        MapReduceResultReceptionConnector.class.getCanonicalName());
    return p;
  }

}
