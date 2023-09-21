package com.pinterest.psc.common;

import com.pinterest.psc.config.PscConfiguration;

public interface PscPlugin {
    default void configure(PscConfiguration pscConfiguration) {
    }
}
