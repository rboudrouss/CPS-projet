package com.rboud.cps.utils;

import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

/**
 * this class exists cause the IntInterval doesn't support split at a given
 * index, it just splits in half.
 * 
 * Still not implemented yet
 * 
 * @see IntInterval
 */
public class MyInterval extends IntInterval {

  public MyInterval(int first, int last) {
    super(first, last);
  }

}
