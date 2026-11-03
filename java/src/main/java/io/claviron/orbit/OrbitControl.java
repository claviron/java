package io.claviron.orbit;

import java.nio.ByteBuffer;

/** Immutable control snapshot. The application closes its native delivery scope after callback return. */
public final class OrbitControl implements AutoCloseable {
    public enum Kind { LIFECYCLE, COMMAND, SHUTDOWN }
    public enum Action { START, STOP, RESTART }
    private final Kind kind;
    private final Action action;
    private final long generation;
    private final boolean running;
    private final String name;
    private final byte[] payload;
    private final String edge;
    private final Runnable release;
    private boolean closed;
    OrbitControl(Kind kind, Action action, long generation, boolean running, String name, byte[] payload, String edge, Runnable release) {
        this.kind = kind; this.action = action; this.generation = generation; this.running = running;
        this.name = name; this.payload = payload; this.edge = edge; this.release = release;
    }
    public Kind kind() { return kind; }
    public Action action() { return action; }
    public long generation() { return generation; }
    public boolean running() { return running; }
    public String name() { return name; }
    public ByteBuffer payload() { return ByteBuffer.wrap(payload).asReadOnlyBuffer(); }
    public String edge() { return edge; }
    @Override public synchronized void close() { if (!closed) { release.run(); closed = true; } }
}
