package com.rboud.cps.utils;

/**
 * Utility class for handling URI stamping operations.
 * This class provides methods to add and extract origin node information from URIs.
 */
public final class URIStamper {

  /**
   * Stamps a node URI onto an original URI by appending it with a '#' separator.
   *
   * @param uri     The original URI to be stamped
   * @param nodeURI The node URI to stamp onto the original URI
   * @return The combined URI with the node URI stamped
   */
  public static String stampOriginNodeToUri(String uri, String nodeURI) {
    return uri + "#" + nodeURI;
  }

  /**
   * Extracts the origin node URI from a stamped URI.
   *
   * @param uri The stamped URI to extract from
   * @return The origin node URI, or null if the URI is not stamped
   */
  public static String getOriginNodeFromUri(String uri) {
    if (!isComputationUriStamped(uri))
      return null;
    return uri.split("#")[1];
  }

  /**
   * Extracts the original computation URI from a stamped URI.
   *
   * @param uri The stamped URI to extract from
   * @return The original computation URI (part before the '#')
   */
  public static String getOriginalComputationUriFromUri(String uri) {
    return uri.split("#")[0];
  }

  /**
   * Checks if a URI has been stamped with an origin node.
   *
   * @param uri The URI to check
   * @return true if the URI contains a '#' character, false otherwise
   */
  public static boolean isComputationUriStamped(String uri) {
    return uri.contains("#");
  }

}
