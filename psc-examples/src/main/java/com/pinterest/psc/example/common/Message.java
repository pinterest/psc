package com.pinterest.psc.example.common;

import java.io.Serializable;

public class Message implements Serializable {
    private static final long serialVersionUID = -8930189990612902608L;
    private int id;
    private String text;
    private long timestamp;

    public Message(int id, String text, long timestamp) {
        this.id = id;
        this.text = text;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("id: %d; text: %s; timestamp: %d", id, text, timestamp);
    }
}
