package com.rboud.cps.connections.endpoints.NodeFacade.NodeAsync;

import com.rboud.cps.connections.connectors.ContentAccessConnector;
import com.rboud.cps.connections.ports.Facade.FacadeContentAccessOutboundPort;
import com.rboud.cps.connections.ports.Node.Async.NodeContentAccessInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

public class NodeFacadeContentAccessEndpoint extends BCMEndPoint<ContentAccessCI> {

  public NodeFacadeContentAccessEndpoint() {
    super(ContentAccessCI.class, ContentAccessCI.class);
  }

  public NodeFacadeContentAccessEndpoint(String inboundPortURI) {
    super(ContentAccessCI.class, ContentAccessCI.class, inboundPortURI);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeContentAccessInboundPort port = new NodeContentAccessInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeContentAccessOutboundPort port = new FacadeContentAccessOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        ContentAccessConnector.class.getCanonicalName());
    return port;
  }

}
