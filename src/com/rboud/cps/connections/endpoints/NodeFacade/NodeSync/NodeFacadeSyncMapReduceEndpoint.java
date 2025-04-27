package com.rboud.cps.connections.endpoints.NodeFacade.NodeSync;

import com.rboud.cps.connections.connectors.MapReduceSyncConnector;
import com.rboud.cps.connections.ports.Facade.FacadeMapReduceSyncOutboundPort;
import com.rboud.cps.connections.ports.Node.Sync.NodeMapReduceSyncInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

/**
 * Endpoint managing synchronous MapReduce operations between nodes and the
 * facade.
 * Creates and manages ports for synchronous MapReduce operations between
 * components.
 */
public class NodeFacadeSyncMapReduceEndpoint extends BCMEndPoint<MapReduceSyncCI> {

  /**
   * Creates a new endpoint for synchronous MapReduce operations with default
   * configuration.
   */
  public NodeFacadeSyncMapReduceEndpoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  /**
   * Creates a new endpoint with a specific inbound port URI.
   *
   * @param inboundPortURI URI for the inbound port
   */
  public NodeFacadeSyncMapReduceEndpoint(String inboundPortURI) {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class, inboundPortURI);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    NodeMapReduceSyncInboundPort port = new NodeMapReduceSyncInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    FacadeMapReduceSyncOutboundPort port = new FacadeMapReduceSyncOutboundPort(c);
    port.publishPort();
    c.doPortConnection(
        port.getPortURI(),
        inboundPortURI,
        MapReduceSyncConnector.class.getCanonicalName());
    return port;
  }

}
