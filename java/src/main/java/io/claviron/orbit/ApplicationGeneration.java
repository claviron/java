package io.claviron.orbit;

import java.util.concurrent.CompletionStage;

/** A captured generation. Retirement never makes a previously captured signal live again. */
public final class ApplicationGeneration {
    private final long id;
    private final CompletionStage<Void> cancelled;
    ApplicationGeneration(long id, CompletionStage<Void> cancelled) { this.id = id; this.cancelled = cancelled; }
    public long id() { return id; }
    /** Completes on stop/restart or after application close; does not cancel arbitrary Java tasks. */
    public CompletionStage<Void> cancelled() { return cancelled; }
}
