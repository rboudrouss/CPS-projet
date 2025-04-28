package com.rboud.cps.components;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.swing.text.AbstractDocument.Content;

import com.rboud.cps.connections.endpoints.NodeNode.Dynamic.NodeNodeDynamicCompositeEndPoint;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class, DHTManagementCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
    MapReduceResultReceptionCI.class })
public class DynamicNode<NodeCompositeEndpointT extends ContentNodeCompositeEndPointI<CAI, MRI, DHTManagementCI>, CAI extends ContentAccessCI, MRI extends ParallelMapReduceCI>
    extends AsyncNode<NodeCompositeEndpointT, CAI, MRI> implements DHTManagementI {

  List<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords;

  List<NodeCompositeEndpointT> createdEndpoints;
  List<NodeCompositeEndpointT> connectedToEndpoints;

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
          best = chord; // Prendre la corde la plus proche sans dépasser
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
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);
    } else {
      super.get(computationURI, key, caller);
    }
  }

  @Override
  public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
      EndPointI<I> caller) throws Exception {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value, caller);
    } else {
      super.put(computationURI, key, value, caller);
    }
  }

  @Override
  public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
      throws Exception {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      bestChord.first().getContentAccessEndpoint().getClientSideReference().remove(computationURI, key, caller);
    } else {
      super.remove(computationURI, key, caller);
    }
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().getSync(URI, key);
    } else {
      return super.getSync(URI, key);
    }
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().putSync(URI, key, value)
    } else {
      return super.putSync(URI, key, value);
    }
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> bestChord = this
        .findBestChord(key.hashCode());
    if (bestChord != null) {
      return bestChord.first().getContentAccessEndpoint().getClientSideReference().removeSync(URI, key);
    } else {
      return super.removeSync(URI, key);
    }
  }

}
