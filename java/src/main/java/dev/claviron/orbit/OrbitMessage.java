package dev.claviron.orbit;

import java.nio.charset.StandardCharsets;

/** One whole message on a session. */
public final class OrbitMessage {
    public enum Kind { TEXT, BINARY, CLOSE }
    private final Kind kind;
    private final byte[] data;
    private final int code;

    OrbitMessage(Kind kind, byte[] data, int code) {
        this.kind = kind;
        this.data = data;
        this.code = code;
    }

    public Kind kind() { return kind; }
    /** The message's bytes, or a close's UTF-8 reason. */
    public byte[] bytes() { return data.clone(); }
    /** The message as text, or a close's reason. */
    public String text() { return new String(data, StandardCharsets.UTF_8); }
    /** A close's code: 1005 means none was given and 1006 that the client left without a close. */
    public int closeCode() { return code; }
}
