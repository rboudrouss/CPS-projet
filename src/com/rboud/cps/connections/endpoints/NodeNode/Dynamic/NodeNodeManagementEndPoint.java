package com.rboud.cps.connections.endpoints.NodeNode.Dynamic;

import com.rboud.cps.connections.connectors.DHTManagementConnector;
import com.rboud.cps.connections.ports.Node.Dynamic.NodeManagementInboundPort;
import com.rboud.cps.connections.ports.Node.Dynamic.NodeManagementOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;

public class NodeNodeManagementEndPoint extends BCMEndPoint<DHTManagementCI> {

  public NodeNodeManagementEndPoint() {
    super(DHTManagementCI.class, DHTManagementCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeManagementInboundPort port = new NodeManagementInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected DHTManagementCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeManagementOutboundPort port = new NodeManagementOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        DHTManagementConnector.class.getCanonicalName());
    return port;
  }

}
