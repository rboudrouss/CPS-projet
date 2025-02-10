package com.rboud.cps.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTNode implements ContentAccessSyncI, MapReduceSyncI {
  private Set<String> seenGetURIs = new HashSet<String>();
  private Set<String> seenPutURIs = new HashSet<String>();
  private Set<String> seenMapURIs = new HashSet<String>();

  // key: computationURI, value: results extended from Array
  private Map<String, ArrayList<?>> mapResults = new HashMap<>();



  private int minHash;
  private int maxHash;

  private DHTNode next;
  private final Map<ContentKeyI, ContentDataI> localStorage = new HashMap<>();

  public DHTNode(DHTNode next, int minHash, int maxHash) {
    this.minHash = minHash;
    this.maxHash = maxHash;
    this.next = next;
  }

  public DHTNode(DHTNode next) {
    this(next, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public DHTNode() {
    this(null);
  }

  @Override
  public ContentDataI getSync(String URI, ContentKeyI key) throws Exception {
    if (seenGetURIs.contains(URI)) {
      System.err.println("WARNING GETSYNC loop detected (or called before previous computation ends) with URI" + URI + ", and hashcode :" + key.hashCode());
    }
    seenGetURIs.add(URI);

    ContentDataI out = null;

    if ((key.hashCode() < minHash || key.hashCode() > maxHash)) {
      if (next != null)
        out = next.getSync(URI, key);
    } else {
      out = localStorage.get(key);
    }

    seenGetURIs.remove(URI);
    return out;
  }

  @Override
  public ContentDataI putSync(String URI, ContentKeyI key, ContentDataI value) throws Exception {
    if (seenPutURIs.contains(URI)) {
      System.err.println("WARNING PUTSYNC loop detected (or called before previous computation ends) with URI" + URI + ", and hashcode :" + key.hashCode());
    }
    seenPutURIs.add(URI);

    ContentDataI out = null;

    if ((key.hashCode() < minHash || key.hashCode() > maxHash)) {
      if (next != null)
        out = next.putSync(URI, key, value);
    } else {
      out = localStorage.put(key, value);
      localStorage.put(key, value);
    }

    seenPutURIs.remove(URI);
    return out;
  }

  @Override
  public ContentDataI removeSync(String URI, ContentKeyI key) throws Exception {
    ContentDataI out = null;

    if ((key.hashCode() < minHash || key.hashCode() > maxHash)) {
      if (next != null)
        out = next.removeSync(URI, key);
    } else {
      out = localStorage.remove(key);
    }

    return out;
  }

  @Override
  public void clearComputation(String URI) throws Exception {
    seenGetURIs.remove(URI);
    seenPutURIs.remove(URI);
  }

  @Override
  public void clearMapReduceComputation(String URI) throws Exception {
    throw new UnsupportedOperationException("Unimplemented method 'clearMapReduceComputation'");
  }

  @Override
  public <R extends Serializable> void mapSync(String URI, SelectorI selector, ProcessorI<R> processor) throws Exception {
    if (seenMapURIs.contains(URI)) {
      System.err.println("?? MAPSYNC This souldn't happen, loop detected (or called again) with URI" + URI);
      throw new Exception("MAPSYNC Loop detected");
    }
    seenMapURIs.add(URI);

    // HACK used Serializable instead of R, idk mapResults.put don't like with R
    ArrayList<Serializable> results = localStorage.values().stream()
        .filter(selector::test)
        .map(processor::apply)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    

    mapResults.put(URI, results);
    seenMapURIs.remove(URI);
  }

  @Override
  public <A extends Serializable, R> A reduceSync(String URI, ReductorI<A, R> reductor, CombinatorI<A> combinator, A acc)
      throws Exception {

        // HACK Il faut vérifier si le cast est possible
        // @SuppressWarnings("unchecked")
        ArrayList<R> data = (ArrayList<R>) mapResults.get(URI);
        if (data == null) {
          throw new Exception("No data found for URI " + URI);
        }

        return data.stream().reduce(acc, reductor::apply, combinator::apply);
  }
}
