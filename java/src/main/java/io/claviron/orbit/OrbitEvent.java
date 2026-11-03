package io.claviron.orbit;

/** ApplicationEvent snapshot and delivery scope; close after manual consumption. */
public final class OrbitEvent implements AutoCloseable {
    private final String id;
    private final String topic;
    private final String json;
    private final Runnable release;
    private boolean closed;

    OrbitEvent(String id, String topic, String json, Runnable release) {
        this.id = id; this.topic = topic; this.json = json; this.release = release;
    }
    public String id() { return id; }
    public String topic() { return topic; }
    public String json() { return json; }
    @Override public synchronized void close() {
        if (!closed) { release.run(); closed = true; }
    }
}
