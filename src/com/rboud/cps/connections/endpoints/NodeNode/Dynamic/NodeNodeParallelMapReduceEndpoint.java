package com.rboud.cps.connections.endpoints.NodeNode.Dynamic;

import com.rboud.cps.components.DynamicNode;
import com.rboud.cps.connections.connectors.MapReduceParallelConnector;
import com.rboud.cps.connections.ports.Node.Dynamic.NodeParallelMapReduceInboundPort;
import com.rboud.cps.connections.ports.Node.Dynamic.NodeParallelMapReduceOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;

public class NodeNodeParallelMapReduceEndpoint extends BCMEndPoint<ParallelMapReduceCI> {

  public NodeNodeParallelMapReduceEndpoint() {
    super(ParallelMapReduceCI.class, ParallelMapReduceCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof DynamicNode;
    NodeParallelMapReduceInboundPort port = new NodeParallelMapReduceInboundPort(inboundPortURI, c,
        ((DynamicNode) c).getMapReduceExecutorServiceURI());
    port.publishPort();
    return port;
  }

  @Override
  protected ParallelMapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    assert c instanceof DynamicNode;
    NodeParallelMapReduceOutboundPort port = new NodeParallelMapReduceOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceParallelConnector.class.getCanonicalName());
    return port;
  }

}
