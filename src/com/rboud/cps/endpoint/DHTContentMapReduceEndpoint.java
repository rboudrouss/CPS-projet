package com.rboud.cps.endpoint;

import com.rboud.cps.ports.ClientMapReduceOutboundPort;
import com.rboud.cps.ports.DHTMapReduceInboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class DHTContentMapReduceEndpoint extends BCMEndPoint<MapReduceSyncCI> {

  public DHTContentMapReduceEndpoint() {
    super(MapReduceSyncCI.class, MapReduceSyncCI.class);
  }

  @Override
  protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    DHTMapReduceInboundPort port = new DHTMapReduceInboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

  @Override
  protected MapReduceSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    ClientMapReduceOutboundPort port = new ClientMapReduceOutboundPort(inboundPortURI, c);
    port.publishPort();
    return port;
  }

}
