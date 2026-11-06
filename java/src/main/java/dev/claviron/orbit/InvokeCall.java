package dev.claviron.orbit;

import java.util.concurrent.CompletionStage;

/** Cancellation withdraws this caller's wait; accepted remote work is not rolled back. */
public final class InvokeCall {
    private final NativeOperation operation;
    private final CompletionStage<InvokeResult> completion;
    InvokeCall(NativeOperation operation, InvokeFfm api) {
        this.operation = operation;
        completion = operation.completion().thenApply(ignored -> api.result(operation.resource));
    }
    public CompletionStage<InvokeResult> completion() { return completion; }
    public boolean cancel() { return operation.cancel(); }
}
