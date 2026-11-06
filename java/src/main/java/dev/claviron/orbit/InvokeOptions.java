package dev.claviron.orbit;

import java.util.Map;
import java.util.Objects;

/** Explicit, separate Link identity and limits; routes bind logical targets once. */
public record InvokeOptions(String fleet, String service, int pendingCalls, int acceptedCalls,
                            int operationBytes, int requestBytes, int responseBytes, Map<String, String> routes) {
    public InvokeOptions {
        Objects.requireNonNull(fleet); Objects.requireNonNull(service);
        routes = Map.copyOf(routes);
        if (fleet.isBlank() || service.isBlank() || pendingCalls <= 0 || acceptedCalls <= 0
                || operationBytes <= 0 || requestBytes < 0 || responseBytes < 0) {
            throw new IllegalArgumentException("Invalid invoke identity or bounds");
        }
    }
}
