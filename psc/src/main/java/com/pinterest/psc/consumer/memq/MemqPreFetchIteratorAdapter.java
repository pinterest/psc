package com.pinterest.psc.consumer.memq;

import com.pinterest.psc.common.CloseableIterator;

import java.io.IOException;
import java.util.List;

public class MemqPreFetchIteratorAdapter<T> implements CloseableIterator<T> {

    private final List<T> messages;
    private int index = 0;

    public MemqPreFetchIteratorAdapter(List<T> messages) {
        this.messages = messages;
    }

    @Override
    public void close() throws IOException {
        messages.clear();
    }

    @Override
    public boolean hasNext() {
        return index < messages.size();
    }

    @Override
    public T next() {
        return messages.get(index++);
    }
}
