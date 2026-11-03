package io.claviron.orbit;

/** An invoke result, independent of HTTP status, headers or streaming bodies. */
public final class InvokeReply {
    private final int code;
    private final byte[] payload;
    private InvokeReply(int code, byte[] payload) { this.code = code; this.payload = payload; }
    public static InvokeReply success(byte[] payload) { return new InvokeReply(0, payload.clone()); }
    public static InvokeReply busy() { return new InvokeReply(1, new byte[0]); }
    public static InvokeReply closing() { return new InvokeReply(2, new byte[0]); }
    public static InvokeReply unsupported() { return new InvokeReply(3, new byte[0]); }
    public static InvokeReply tooLarge() { return new InvokeReply(4, new byte[0]); }
    public static InvokeReply rejected(byte[] payload) { return new InvokeReply(5, payload.clone()); }
    int code() { return code; }
    byte[] bytes() { return payload; }
}
