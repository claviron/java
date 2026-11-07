package dev.claviron.orbit;

import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One accepted message stream. One receive and one send may be pending at a time;
 * another fails with Busy until it completes. A received close is answered natively.
 */
public final class OrbitSession {
    private static final int TEXT = 1, BINARY = 2, CLOSE = 3;
    private static final int HEAD = 3;
    private final OrbitRequest request;
    private CompletionStage<Void> ending;

    OrbitSession(OrbitRequest request) { this.request = request; }

    /** The next message, or null after the last one. A message over the limit ends the session with status 4. */
    public CompletionStage<OrbitMessage> receive() {
        return request.readMessage().thenApply(lease -> {
            if (lease == null) return null;
            try (lease) { return decode(lease.view().toArray(ValueLayout.JAVA_BYTE)); }
        });
    }

    private static OrbitMessage decode(byte[] record) {
        OrbitMessage.Kind kind = switch (record[0]) {
            case TEXT -> OrbitMessage.Kind.TEXT;
            case BINARY -> OrbitMessage.Kind.BINARY;
            case CLOSE -> OrbitMessage.Kind.CLOSE;
            default -> throw new NativeException(9);
        };
        int code = (record[1] & 0xff) | (record[2] & 0xff) << 8;
        return new OrbitMessage(kind, Arrays.copyOfRange(record, HEAD, record.length), code);
    }

    public synchronized CompletionStage<Void> sendText(String text) {
        return request.sendMessage(TEXT, 0, text.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized CompletionStage<Void> sendBinary(byte[] bytes) {
        return request.sendMessage(BINARY, 0, bytes);
    }

    /** Send a close message and end the session. Later calls return the same stage. */
    public synchronized CompletionStage<Void> close(int code, String reason) {
        if (ending == null) ending = request.sendMessage(CLOSE, code, reason.getBytes(StandardCharsets.UTF_8));
        return ending;
    }

    /** End the session at once, without a close message. */
    public void abort() { request.close(); }

    /** Completes when the peer or the application generation ended the session uncleanly. */
    public CompletionStage<Void> cancelled() { return request.cancelled(); }

    synchronized CompletionStage<Void> finish() {
        if (ending != null) return ending;
        try { ending = request.finishResponse(); }
        catch (NativeException alreadyEnded) { ending = CompletableFuture.completedStage(null); }
        return ending;
    }
}
