package com.pinterest.psc.common;

/**
 * Represents headers available for PSC events
 */
public enum PscEventHeaders {

  EVENT_HEADER("__EVENT_HEADER");

  private final String value;

  PscEventHeaders(String value) {
    this.value = value;
  }

  public String getValue(){
    return value;
  }
}
