package com.rboud.cps.components;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import com.rboud.cps.connections.endpoints.NodeNode.Dynamic.NodeNodeDynamicCompositeEndPoint;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

@OfferedInterfaces(offered = { ParallelMapReduceCI.class, ContentAccessCI.class, DHTManagementCI.class,
    MapReduceResultReceptionCI.class })
@RequiredInterfaces(required = { ParallelMapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
    MapReduceResultReceptionCI.class })
public class DynamicNode<NodeCompositeEndpointT extends ContentNodeCompositeEndPointI<CAI, MRI, DHTManagementCI>, CAI extends ContentAccessCI, MRI extends ParallelMapReduceCI>
    extends AsyncNode<NodeCompositeEndpointT, CAI, MRI>
    implements DHTManagementI, ParallelMapReduceI, MapReduceResultReceptionI {

  final static int NB_CHORDS = 4;

  List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords;

  List<NodeCompositeEndpointT> createdEndpoints;
  List<NodeCompositeEndpointT> connectedToEndpoints;

  protected ConcurrentHashMap<String, CompletableFuture<Serializable>> computationResults = new ConcurrentHashMap<>();

  protected DynamicNode(NodeCompositeEndpointT nodeFacadeCompositeEndpoint,
      NodeCompositeEndpointT selfNodeCompositeEndpoint,
      NodeCompositeEndpointT nextNodeCompositeEndpoint) throws Exception {
    super(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint);

    assert this.chords != null : "chords should not be null";
    assert this.createdEndpoints != null : "createdEndpoits should not be null";
  }

  @Override
  protected void initialise(NodeCompositeEndpointT nodeFacadeCompositeEndpoint,
      NodeCompositeEndpointT selfNodeCompositeEndpoint, NodeCompositeEndpointT nextNodeCompositeEndpoint, int minValue,
      int maxValue) throws Exception {
    super.initialise(nodeFacadeCompositeEndpoint, selfNodeCompositeEndpoint, nextNodeCompositeEndpoint, minValue,
        maxValue);
    this.chords = new CopyOnWriteArrayList<>();
    this.createdEndpoints = new CopyOnWriteArrayList<>();
  }

  @Override
  protected void cleanUpConnections() throws Exception {
    super.cleanUpConnections();
    for (NodeCompositeEndpointT endpoint : this.createdEndpoints) {
      endpoint.cleanUpServerSide();
    }
  }

  @Override
  public synchronized void start() throws ComponentStartException {
    super.start();
    try {
      this.computeChords(URIGenerator.generateURI(), NB_CHORDS);
    } catch (Exception e) {
      throw new ComponentStartException("Error while starting the node", e);
    }
  }

  // ------------------------------------------------------------------------
  // DHTManagementI methods
  // ------------------------------------------------------------------------

  @Override
  public void initialiseContent(NodeContentI content) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'initialiseContent'");
  }

  @Override
  public NodeStateI getCurrentState() throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getCurrentState'");
  }

  @Override
  public NodeContentI suppressNode() throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'suppressNode'");
  }

  @Override
  public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'split'");
  }

  @Override
  public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy, EndPointI<CI> caller)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'merge'");
  }

  @Override
  public void computeChords(String computationURI, int numberOfChords) throws Exception {
    List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> newChords = new CopyOnWriteArrayList<>();
    for (int i = 0; i < numberOfChords; i++) {
      int offset = (int) Math.pow(2, i);
      SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chordInfo = this
          .getChordInfo(offset);
      newChords.add(chordInfo);
    }

    for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : newChords) {
      ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> endpoint = chord.first();
      endpoint.initialiseServerSide(this);
      this.connectedToEndpoints.add((NodeCompositeEndpointT) endpoint); // FIXME note that this is not unsafe because
                                                                        // they are the same
    }
    this.chords.clear();
    this.chords.addAll(newChords);

  }

  @Override
  public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
      int offset) throws Exception {
    if (offset < 0) {
      throw new IllegalArgumentException("Offset cannot be negative");
    } else if (offset == 0) {
      ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> endpoint = new NodeNodeDynamicCompositeEndPoint();
      createdEndpoints.add((NodeCompositeEndpointT) endpoint); // FIXME note that this is not unsafe because they are
                                                               // litterally the same
      endpoint.initialiseServerSide(this);
      return new SerializablePair<>(endpoint, this.interval.first());
    } else {
      return this.getNextDhtManagementReference().getChordInfo(offset - 1);
    }
  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------
  protected DHTManagementCI getNextDhtManagementReference() {
    return this.nextNodeCompositeEndpoint.getDHTManagementEndpoint().getClientSideReference();
  }

  private SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> findBestChord(
      int hash) {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> best = null;
    for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : this.chords) {
      if (chord.second() <= hash) {
        if (best == null || chord.second() > best.second()) {
          best = chord;
        }
      }
    }
    return best;
  }

  // ------------------------------------------------------------------------
  // ContentAccessI methods
  // ------------------------------------------------------------------------

  @Override
  public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    if (this.interval.in(key.hashCode())) {
      this.sendResult("GET", caller, computationURI, this.localStorage.get(key));
      return;
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);
    } else {
      this.getNextContentAccessReference().get(computationURI, key, caller);
    }
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    if (this.interval.in(key.hashCode())) {
      this.sendResult("PUT", caller, computationURI, this.localStorage.put(key, value));
      return;
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value, caller);
    } else {
      this.getNextContentAccessReference().put(computationURI, key, value, caller);
    }
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    if (this.interval.in(key.hashCode())) {
      this.sendResult("REMOVE", caller, computationURI, this.localStorage.remove(key));
      return;
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().remove(computationURI, key, caller);
    } else {
      this.getNextContentAccessReference().remove(computationURI, key, caller);
    }
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    if (this.interval.in(key.hashCode())) {
      return this.localStorage.get(key);
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().getSync(URI, key);
    } else {
      return this.getNextContentAccessReference().getSync(URI, key);
    }
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    if (this.interval.in(key.hashCode())) {
      return this.localStorage.put(key, value);
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().putSync(URI, key, value);
    } else {
      return this.getNextContentAccessReference().putSync(URI, key, value);
    }
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    if (this.interval.in(key.hashCode())) {
      return this.localStorage.remove(key);
    }
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().removeSync(URI, key);
    } else {
      return this.getNextContentAccessReference().removeSync(URI, key);
    }
  }

  // ------------------------------------------------------------------------
  // ParallelMapReduceI methods
  // ------------------------------------------------------------------------

  @Override
  public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
      ParallelismPolicyI parallelismPolicy) throws Exception {
    mapResults.compute(computationURI, (uri, existingFuture) -> {
      if (existingFuture != null) {
        return existingFuture;
      }

      try {
        for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : this.chords) {
          chord.first().getMapReduceEndpoint().getClientSideReference()
              .parallelMap(computationURI, selector, processor, parallelismPolicy);
        }
      } catch (Exception e) {
        this.logMessage("[NODE-MAP] MAP ERROR sending to next node");
        e.printStackTrace();
      }

      return CompletableFuture.supplyAsync(() -> this.localStorage.values().stream().parallel()
          .filter(selector)
          .map(processor));
    });
  }

  @Override
  public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
      ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
      ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
    // FIXME J'ai fait ça comme ça pour le moment parce que j'utilise les binaries
    // de CPS, donc modifier ParallelismPolicyI c'est compliqué
    assert parallelismPolicy instanceof DefaultParallelismPolicy
        : "parallelismPolicy should be an instance of DefaultParallelismPolicy";
    mapResults.compute(computationURI, (uri, existingFuture) -> {
      DefaultParallelismPolicy defaultParallelismPolicy = (DefaultParallelismPolicy) parallelismPolicy;
      this.logMessage("[NODE-REDUCE] Reducing with URI: " + computationURI + " and accumulator: " + currentAcc);

      if (this.interval.first() > defaultParallelismPolicy.maxHash) {
        this.logMessage("[Node-reduce] something went wrong, got called but my hash is greater than maxHash");
        return null;
      }

      if (existingFuture == null) {
        this.logMessage("[NODE-REDUCE] something went wrong, got called but no future");
        return null;
      }

      List<CompletableFuture<Serializable>> futures = new CopyOnWriteArrayList<>();

      int index = 0;
      defaultParallelismPolicy.computeOffsets(this.chords).forEach(chord -> {
        try {
          String newComputationURI = computationURI + '@' + index;
          CompletableFuture<Serializable> future = new CompletableFuture<>();
          this.computationResults.put(newComputationURI, future);
          chord.first().getMapReduceEndpoint().getClientSideReference()
              .parallelReduce(newComputationURI, reductor, combinator, identityAcc, currentAcc,
                  defaultParallelismPolicy.newInstance(chord.second(), this.nodeURI), caller); // TODO caller should be
                                                                                               // a endpoint to this
                                                                                               // node
        } catch (Exception e) {
          this.logMessage("[NODE-REDUCE] REDUCE ERROR sending to next node");
          e.printStackTrace();
        }
      });

      List<A> results = (List<A>) futures.stream().parallel()
          .map(CompletableFuture::join)
          .collect(Collectors.toList());

      A finalResult = results.stream().reduce(identityAcc, (acc, result) -> {
        try {
          return combinator.apply(acc, result);
        } catch (Exception e) {
          this.logMessage("[NODE-REDUCE] REDUCE ERROR");
          e.printStackTrace();
          return null;
        }
      });

      this.logMessage("[NODE-REDUCE] Final result: " + finalResult);
      try {
        this.sendResult("REDUCE", caller, computationURI, this.nodeURI, finalResult);
      } catch (Exception e) {
        this.logMessage("[NODE-REDUCE] REDUCE ERROR sending to caller");
        e.printStackTrace();
      }
      return null;
    });
  }

  // ------------------------------------------------------------------------
  // MapReduceResultReceptionI methods
  // ------------------------------------------------------------------------

  @Override
  public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
    this.logMessage("[NODE-accept] " + computationURI + " from " + emitterId);
    CompletableFuture<Serializable> future = this.computationResults.remove(computationURI);
    if (future != null) {
      future.complete(acc);
    } else {
      throw new Exception("No computation result found for URI: " + computationURI);
    }
  }

}

class DefaultParallelismPolicy implements ParallelismPolicyI {
  protected int maxHash;
  protected String originUri;

  DefaultParallelismPolicy(int maxHash, String originUri) {
    this.originUri = originUri;
    this.maxHash = maxHash;
  }

  public DefaultParallelismPolicy newInstance(int maxHash, String nodeUri) {
    return new DefaultParallelismPolicy(this.maxHash, this.originUri == null ? nodeUri : this.originUri);
  }

  List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> computeOffsets(
      List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords) {

    List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> offsets = new CopyOnWriteArrayList<>();
    for (int i = 0; i < chords.size() - 1; i++) {
      SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord = chords
          .get(i), nextChord = chords.get((i + 1));
      if (chord.second() < this.maxHash) {
        offsets.add(new SerializablePair<>(chord.first(), nextChord.second()));
      }
    }
    return offsets;
  }

}