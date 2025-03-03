package com.rboud.cps.core;

import java.io.Serializable;

import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

@RequiredInterfaces(required = { MapReduceSyncCI.class, ContentAccessSyncCI.class })
@OfferedInterfaces(offered = { DHTServicesCI.class })
public class DHTFacade extends AbstractComponent implements DHTServicesCI {
  private NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint;

  private final static String URI_PREFIX = "dht-facade-";
  private final String outboundPortURI;

  protected DHTFacade(NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint) {
    super(1, 0);
    this.nodeFacadeCompositeEndpoint = nodeFacadeCompositeEndpoint;
    this.outboundPortURI = URIGenerator.generateURI(URI_PREFIX);
  }

  @Override
  public synchronized void start() throws ComponentStartException {
    super.start();
    try {
      this.nodeFacadeCompositeEndpoint.initialiseClientSide(this);
    } catch (Exception e) {
      throw new ComponentStartException(e);
    }
  }

  private EndPointI<ContentAccessSyncCI> getContentAccessEndPoint() {
    return this.nodeFacadeCompositeEndpoint.getContentAccessEndpoint();
  }

  private EndPointI<MapReduceSyncCI> getMapReduceEndPoint() {
    return this.nodeFacadeCompositeEndpoint.getMapReduceEndpoint();
  }

  @Override
  public ContentDataI get(ContentKeyI key) throws Exception {
    return this.getContentAccessEndPoint().getClientSideReference().getSync(outboundPortURI, key);
  }

  @Override
  public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    return this.getContentAccessEndPoint().getClientSideReference().putSync(outboundPortURI, key, value);
  }

  @Override
  public ContentDataI remove(ContentKeyI key) throws Exception {
    return this.getContentAccessEndPoint().getClientSideReference().removeSync(outboundPortURI, key);
  }

  @Override
  public <R extends Serializable, A extends Serializable> A mapReduce(
      SelectorI selector,
      ProcessorI<R> processor,
      ReductorI<A, R> reductor,
      CombinatorI<A> combinator,
      A initialAcc) throws Exception {
    assert this.getMapReduceEndPoint().serverSideInitialised();

    this.getMapReduceEndPoint().getClientSideReference().mapSync(outboundPortURI, selector, processor);
    return this.getMapReduceEndPoint().getClientSideReference().reduceSync(outboundPortURI, reductor, combinator,
        initialAcc);
  }

}
