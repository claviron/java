package dev.claviron.orbit;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A client asking for a message session. Claviron owns its handshake and has not answered it yet. */
public final class OrbitSessionRequest {
    /** The protocol the client spoke to Claviron; both carry the same messages. */
    public enum Kind { WEBSOCKET, WEBTRANSPORT }
    private final OrbitRequest request;
    private OrbitSession session;
    private boolean answered;

    OrbitSessionRequest(OrbitRequest request) { this.request = request; }

    public OrbitRequest request() { return request; }
    public Kind kind() { return request.httpVersion().equals("3") ? Kind.WEBTRANSPORT : Kind.WEBSOCKET; }

    /** Start the session with an explicit per-message limit. Each pending receive reserves it from the runtime's byte budget. */
    public synchronized CompletionStage<OrbitSession> accept(int maxMessageBytes, List<Header> headers) {
        if (maxMessageBytes <= 0) throw new IllegalArgumentException("maxMessageBytes must be positive");
        answer();
        OrbitSession accepted = new OrbitSession(request);
        session = accepted;
        return request.acceptSession(maxMessageBytes, headers).thenApply(ignored -> accepted);
    }

    /** Answer the client with a redirect or an error status. */
    public synchronized CompletionStage<Void> reject(int status, List<Header> headers) {
        if (status < 300 || status > 599) throw new IllegalArgumentException("A session rejection requires a 3xx-5xx status");
        answer();
        return request.respond(OrbitResponse.copyOf(status, headers, new byte[0]));
    }

    private void answer() {
        if (answered) throw new IllegalStateException("This session request was already answered");
        answered = true;
    }

    /** Closes what the handler left open once it has completed. */
    synchronized CompletionStage<Void> settle(boolean failed) {
        if (!answered) return reject(failed ? 500 : 501, List.of());
        if (session == null) return CompletableFuture.completedStage(null);
        if (failed) { session.abort(); return CompletableFuture.completedStage(null); }
        return session.finish();
    }
}
