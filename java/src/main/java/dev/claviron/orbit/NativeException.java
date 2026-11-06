package dev.claviron.orbit;

/** A native ABI status, independent of a Java framework or thread-local errno. */
public final class NativeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int status;

    NativeException(int status) {
        super("Orbit native status " + status + ": " + switch (status) {
            case 1 -> "invalid argument";
            case 2 -> "invalid or released handle";
            case 3 -> "wrong handle kind";
            case 4 -> "capacity exhausted";
            case 5 -> "resource busy";
            case 6 -> "closed";
            case 7 -> "cancelled";
            case 8 -> "native panic";
            case 9 -> "incompatible ABI";
            case 10 -> "end of completions";
            case 11 -> "operation already completed";
            case 12 -> "Orbit transport failure";
            default -> "unknown status";
        });
        this.status = status;
    }

    public int status() {
        return status;
    }
}
