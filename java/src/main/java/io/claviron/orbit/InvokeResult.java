package io.claviron.orbit;

import java.nio.ByteBuffer;

/** Completed protocol outcome; outcome unknown never means safe replay. Payload bytes are opaque. */
public final class InvokeResult {
    public enum Code { SUCCESS, UNKNOWN_TARGET, AMBIGUOUS_TARGET, UNAVAILABLE, BUSY, CLOSED, OUTCOME_UNKNOWN,
        PAYLOAD_TOO_LARGE, PROTOCOL, TRANSPORT, REMOTE_BUSY, REMOTE_CLOSING, UNSUPPORTED, RESPONSE_TOO_LARGE, REJECTED, INVALID_CONFIG }
    private final Code code;
    private final byte[] payload;
    InvokeResult(Code code, byte[] payload) { this.code = code; this.payload = payload; }
    public Code code() { return code; }
    public boolean successful() { return code == Code.SUCCESS; }
    public ByteBuffer payload() { return ByteBuffer.wrap(payload).asReadOnlyBuffer(); }
}
