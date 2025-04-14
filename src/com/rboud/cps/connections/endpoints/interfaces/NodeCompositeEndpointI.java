package com.rboud.cps.connections.endpoints.interfaces;


import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

/*
 * This interface is a marker interface that extends the ContentNodeBaseCompositeEndPointI
 * with the correct types for the content access and map reduce endpoints.
 * It does not add any new methods or functionality.
 * The purpose of this interface is to provide a common type for the composite endpoint
 * that isn't 70 characters long.
 */
public interface NodeCompositeEndpointI extends ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> {}
