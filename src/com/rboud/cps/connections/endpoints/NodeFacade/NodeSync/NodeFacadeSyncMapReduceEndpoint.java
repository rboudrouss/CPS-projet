package com.rboud.cps.connections.endpoints.NodeFacade.NodeSync;

import com.rboud.cps.connections.connectors.MapReduceSyncConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceSyncOutboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class NodeFacadeSyncMapReduceEndpoint extends BCMEndPoint<MapReduceSyncCI> {

  public NodeFacadeSyncMapReduceEndpoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  public NodeFacadeSyncMapReduceEndpoint(String inboundPortURI) {
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
    FacadeMapReduceSyncOutboundPort port = new FacadeMapReduceSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
      port.getPortURI(),
      inboundPortURI,
      MapReduceSyncConnector.class.getCanonicalName()
    );
    return port;
  }

}
