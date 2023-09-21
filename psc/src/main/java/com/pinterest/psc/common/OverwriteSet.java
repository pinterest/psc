package com.pinterest.psc.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class OverwriteSet {
    private Set<Object> set = new HashSet<>(4);

    public Set<Object> put(Collection items) {
        set.clear();
        set.addAll(items);
        return set;
    }

    public Set<Object> put(Object item) {
        set.clear();
        set.add(item);
        return set;
    }
}
