package dev.claviron.orbit;

import java.util.Objects;

/** Explicit application identity and retained-work budgets; each JVM gets its own lane. */
public record ApplicationOptions(String fleet, String name, long incarnation, int maxExchanges, int eventCapacity, int controlCapacity) {
    public ApplicationOptions {
        Objects.requireNonNull(fleet);
        Objects.requireNonNull(name);
        if (fleet.isBlank() || name.isBlank() || incarnation <= 0 || maxExchanges <= 0 || eventCapacity <= 0 || controlCapacity <= 0) {
            throw new IllegalArgumentException("Application identity and capacities must be positive/nonempty");
        }
    }
}
