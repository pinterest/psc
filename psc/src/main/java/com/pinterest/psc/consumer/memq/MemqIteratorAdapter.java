package com.pinterest.psc.consumer.memq;

import com.pinterest.psc.common.CloseableIterator;

import java.io.IOException;
import java.util.function.Consumer;

public class MemqIteratorAdapter<T> implements CloseableIterator<T> {

    com.pinterest.memq.commons.CloseableIterator<T> original;

    public MemqIteratorAdapter(com.pinterest.memq.commons.CloseableIterator<T> original) {
        this.original = original;
    }

    @Override
    public void close() throws IOException {
        original.close();
    }

    @Override
    public boolean hasNext() {
        return original.hasNext();
    }

    @Override
    public T next() {
        return original.next();
    }

    @Override
    public void remove() {
        original.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        original.forEachRemaining(action);
    }
}
